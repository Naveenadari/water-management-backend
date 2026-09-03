/* ==========================================================
   WATER MANAGEMENT SYSTEM - BACKEND SERVER
   (HTTP + WebSocket + Auth + Razorpay + Supabase persistence)
   ========================================================== */

const express = require("express");
const cors = require("cors");
const crypto = require("crypto");
const http = require("http");
const WebSocket = require("ws");
const Razorpay = require("razorpay");
const { createClient } = require("@supabase/supabase-js");

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static("public"));

const PORT = process.env.PORT || 3000;

// ---------------- SUPABASE SETUP ----------------
const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || "";

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("WARNING: SUPABASE_URL / SUPABASE_SERVICE_KEY not set. The server will not work until these are configured.");
}
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ---------------- PAYMENT CONFIG ----------------
const RAZORPAY_KEY_ID = process.env.RAZORPAY_KEY_ID || "";
const RAZORPAY_KEY_SECRET = process.env.RAZORPAY_KEY_SECRET || "";
const SUBSCRIPTION_AMOUNT_PAISE = parseInt(process.env.SUBSCRIPTION_AMOUNT_PAISE || "9900", 10);
const SUBSCRIPTION_DAYS = 30;

let razorpay = null;
if (RAZORPAY_KEY_ID && RAZORPAY_KEY_SECRET) {
  razorpay = new Razorpay({ key_id: RAZORPAY_KEY_ID, key_secret: RAZORPAY_KEY_SECRET });
}

// ---------------- WEBSOCKET (live ESP32 connections — kept in memory, ephemeral by nature) ----------------
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/device-ws" });
const deviceSockets = {};       // key -> live WebSocket
const pendingCommands = {};     // key -> "OPEN" | "CLOSE" | null (fallback polling queue, ephemeral)

wss.on("connection", (ws) => {
  let deviceKey = null;
  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }
    if (msg.type === "register") {
      deviceKey = keyFor(msg.apartment, msg.floor, msg.flat);
      deviceSockets[deviceKey] = ws;
      console.log(`Device connected via WebSocket: ${deviceKey}`);
    }
  });
  ws.on("close", () => {
    if (deviceKey && deviceSockets[deviceKey] === ws) {
      delete deviceSockets[deviceKey];
      console.log(`Device disconnected: ${deviceKey}`);
    }
  });
});

// ---------------- HELPERS ----------------
function keyFor(apartment, floor, flat) { return `${apartment}/${floor}/${flat}`; }
function hashPassword(password, salt) { return crypto.scryptSync(password, salt, 64).toString("hex"); }
function makeToken() { return crypto.randomBytes(24).toString("hex"); }
function todayStr(date = new Date()) { return date.toISOString().slice(0, 10); }
function ah(fn) { return (req, res, next) => fn(req, res, next).catch((e) => { console.error(e); res.status(500).json({ error: "Server error" }); }); }

// ---------------- DB ACCESS HELPERS ----------------
async function dbGetUser(phone) {
  const { data } = await supabase.from("users").select("*").eq("phone", phone).maybeSingle();
  return data;
}
async function dbCreateUser(u) {
  const { error } = await supabase.from("users").insert({
    phone: u.phone, role: u.role, name: u.name, salt: u.salt, password_hash: u.passwordHash,
    apartment: u.apartment || null, floor: u.floor || null, flat: u.flat || null,
  });
  if (error) throw error;
}
async function dbUpdateUserPassword(phone, salt, passwordHash) {
  await supabase.from("users").update({ salt, password_hash: passwordHash }).eq("phone", phone);
}
async function dbDeleteUser(phone) {
  await supabase.from("users").delete().eq("phone", phone);
}
async function dbAnySuperAdmin() {
  const { count } = await supabase.from("users").select("phone", { count: "exact", head: true }).eq("role", "super_admin");
  return (count || 0) > 0;
}
async function dbGetFlatOwnerByKey(apartment, floor, flat) {
  const { data } = await supabase.from("users").select("*").eq("role", "flat_owner")
    .eq("apartment", apartment).eq("floor", floor).eq("flat", flat).maybeSingle();
  return data;
}
async function dbGetAllFlatOwners() {
  const { data } = await supabase.from("users").select("*").eq("role", "flat_owner");
  return data || [];
}

async function dbCreateSession(token, phone) { await supabase.from("sessions").insert({ token, phone }); }
async function dbGetSessionPhone(token) {
  const { data } = await supabase.from("sessions").select("phone").eq("token", token).maybeSingle();
  return data ? data.phone : null;
}
async function dbDeleteSessionsForPhone(phone) { await supabase.from("sessions").delete().eq("phone", phone); }

async function dbCreateFlatInvite(token, apartment, floor, flat) {
  await supabase.from("flat_invites").insert({ token, apartment, floor, flat });
}
async function dbGetFlatInvite(token) {
  const { data } = await supabase.from("flat_invites").select("*").eq("token", token).maybeSingle();
  return data;
}
async function dbMarkFlatInviteUsed(token) { await supabase.from("flat_invites").update({ used: true }).eq("token", token); }

async function dbCreateAdminInvite(token) { await supabase.from("admin_invites").insert({ token }); }
async function dbGetAdminInvite(token) {
  const { data } = await supabase.from("admin_invites").select("*").eq("token", token).maybeSingle();
  return data;
}
async function dbMarkAdminInviteUsed(token) { await supabase.from("admin_invites").update({ used: true }).eq("token", token); }

async function dbGetLimit(key) {
  const { data } = await supabase.from("limits").select("daily_limit_liters").eq("key", key).maybeSingle();
  return data ? Number(data.daily_limit_liters) : null;
}
async function dbSetLimit(key, value) { await supabase.from("limits").upsert({ key, daily_limit_liters: value }); }

async function dbGetSubscriptionPaidUntil(key) {
  const { data } = await supabase.from("subscriptions").select("paid_until").eq("key", key).maybeSingle();
  return data ? data.paid_until : null;
}
async function dbSetSubscription(key, paidUntilISO) { await supabase.from("subscriptions").upsert({ key, paid_until: paidUntilISO }); }
async function isSubscriptionActive(key) {
  const paidUntil = await dbGetSubscriptionPaidUntil(key);
  if (!paidUntil) return false;
  return new Date(paidUntil) > new Date();
}

async function dbUpsertFlatData(key, apartment, floor, flat, record) {
  await supabase.from("flats_data").upsert({
    key, apartment, floor, flat,
    flow_lpm: record.flow_lpm, total_liters: record.total_liters, valve_status: record.valve_status,
    received_at: record.received_at,
  });
}
async function dbGetFlatData(key) {
  const { data } = await supabase.from("flats_data").select("*").eq("key", key).maybeSingle();
  return data;
}
async function dbGetAllFlatsData() {
  const { data } = await supabase.from("flats_data").select("*");
  return data || [];
}

async function dbInsertReading(key, record) {
  await supabase.from("readings_history").insert({
    key, flow_lpm: record.flow_lpm, total_liters: record.total_liters,
    valve_status: record.valve_status, received_at: record.received_at,
  });
}
async function dbGetHistory(key, limit = 200) {
  const { data } = await supabase.from("readings_history").select("*").eq("key", key)
    .order("received_at", { ascending: false }).limit(limit);
  return (data || []).reverse();
}

async function dbAddDailyUsage(key, day, delta) {
  const { data } = await supabase.from("daily_usage").select("liters").eq("key", key).eq("usage_date", day).maybeSingle();
  const newVal = (data ? Number(data.liters) : 0) + delta;
  await supabase.from("daily_usage").upsert({ key, usage_date: day, liters: newVal });
  return newVal;
}
async function dbGetDailyUsage(key, day) {
  const { data } = await supabase.from("daily_usage").select("liters").eq("key", key).eq("usage_date", day).maybeSingle();
  return data ? Number(data.liters) : 0;
}
async function dbGetAllDailyUsage(key) {
  const { data } = await supabase.from("daily_usage").select("usage_date, liters").eq("key", key).order("usage_date", { ascending: true });
  return (data || []).map((r) => ({ date: r.usage_date, liters: Number(r.liters) }));
}

async function dbGetAlertsSent(key) {
  const { data } = await supabase.from("alerts_sent").select("*").eq("key", key).maybeSingle();
  return data;
}
async function dbSetAlertsSent(key, day, level80, level100) {
  await supabase.from("alerts_sent").upsert({ key, alert_day: day, level80, level100 });
}

async function dbInsertNotification(key, level, message) {
  await supabase.from("notifications").insert({ key, level, message });
}
async function dbGetAdminNotifications() {
  const { data } = await supabase.from("notifications").select("*").order("created_at", { ascending: false }).limit(50);
  return (data || []).map((n) => ({ key: n.key, level: n.level, message: n.message, timestamp: n.created_at }));
}
async function dbGetFlatNotifications(key) {
  const { data } = await supabase.from("notifications").select("*").eq("key", key).order("created_at", { ascending: false }).limit(50);
  return (data || []).map((n) => ({ level: n.level, message: n.message, timestamp: n.created_at }));
}

async function dbGetPreviousTotal(key) {
  const { data } = await supabase.from("previous_total").select("total_liters").eq("key", key).maybeSingle();
  return data ? Number(data.total_liters) : null;
}
async function dbSetPreviousTotal(key, value) { await supabase.from("previous_total").upsert({ key, total_liters: value }); }

async function dbSaveAdminOrder(orderId, keys) { await supabase.from("admin_orders").insert({ order_id: orderId, flat_keys: keys }); }
async function dbGetAdminOrder(orderId) {
  const { data } = await supabase.from("admin_orders").select("flat_keys").eq("order_id", orderId).maybeSingle();
  return data ? data.flat_keys : null;
}
async function dbDeleteAdminOrder(orderId) { await supabase.from("admin_orders").delete().eq("order_id", orderId); }

// ---------------- AUTH MIDDLEWARE ----------------
async function requireAuth(req, res, next) {
  try {
    const authHeader = req.headers.authorization || "";
    const token = authHeader.replace("Bearer ", "");
    const phone = await dbGetSessionPhone(token);
    if (!phone) return res.status(401).json({ error: "Not authenticated" });
    const user = await dbGetUser(phone);
    if (!user) return res.status(401).json({ error: "Not authenticated" });
    req.user = { role: user.role, name: user.name, phone: user.phone, apartment: user.apartment, floor: user.floor, flat: user.flat };
    next();
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Server error" });
  }
}
function requireAdmin(req, res, next) {
  if (req.user.role !== "admin" && req.user.role !== "super_admin") return res.status(403).json({ error: "Admin only" });
  next();
}
function requireSuperAdmin(req, res, next) {
  if (req.user.role !== "super_admin") return res.status(403).json({ error: "Super admin only" });
  next();
}

// ================= AUTH ROUTES =================

const SUPER_ADMIN_BOOTSTRAP_KEY = process.env.SUPER_ADMIN_BOOTSTRAP_KEY || "";

app.post("/api/auth/bootstrap-superadmin", ah(async (req, res) => {
  const { name, phone, password, bootstrap_key } = req.body;

  if (await dbAnySuperAdmin()) return res.status(403).json({ error: "A super admin account already exists." });
  if (!SUPER_ADMIN_BOOTSTRAP_KEY || bootstrap_key !== SUPER_ADMIN_BOOTSTRAP_KEY) return res.status(403).json({ error: "Invalid bootstrap key." });
  if (!name || !phone || !password) return res.status(400).json({ error: "Missing required fields" });
  if (await dbGetUser(phone)) return res.status(409).json({ error: "An account with this phone number already exists" });

  const salt = crypto.randomBytes(16).toString("hex");
  const passwordHash = hashPassword(password, salt);
  await dbCreateUser({ role: "super_admin", name, phone, salt, passwordHash });

  const token = makeToken();
  await dbCreateSession(token, phone);
  res.json({ success: true, token, user: { role: "super_admin", name, phone, apartment: null, floor: null, flat: null } });
}));

// ---------------- INVITE-BASED ADMIN SIGNUP ----------------

app.post("/api/admin-invites", requireAuth, requireSuperAdmin, ah(async (req, res) => {
  const token = crypto.randomBytes(12).toString("hex");
  await dbCreateAdminInvite(token);
  res.json({ success: true, token });
}));

app.get("/api/admin-invites/:token", ah(async (req, res) => {
  const invite = await dbGetAdminInvite(req.params.token);
  if (!invite) return res.status(404).json({ error: "Invalid or expired invite link" });
  if (invite.used) return res.status(410).json({ error: "This invite link has already been used" });
  res.json({ valid: true });
}));

app.post("/api/auth/admin-signup-with-invite", ah(async (req, res) => {
  const { token, name, phone, password } = req.body;
  const invite = await dbGetAdminInvite(token);

  if (!invite) return res.status(404).json({ error: "Invalid or expired invite link" });
  if (invite.used) return res.status(410).json({ error: "This invite link has already been used" });
  if (!name || !phone || !password) return res.status(400).json({ error: "Missing required fields" });
  if (await dbGetUser(phone)) return res.status(409).json({ error: "An account with this phone number already exists" });

  const salt = crypto.randomBytes(16).toString("hex");
  const passwordHash = hashPassword(password, salt);
  await dbCreateUser({ role: "admin", name, phone, salt, passwordHash });
  await dbMarkAdminInviteUsed(token);

  const authToken = makeToken();
  await dbCreateSession(authToken, phone);
  res.json({ success: true, token: authToken, user: { role: "admin", name, phone, apartment: null, floor: null, flat: null } });
}));

// ---------------- INVITE-BASED FLAT OWNER SIGNUP ----------------

app.post("/api/invites", requireAuth, requireAdmin, ah(async (req, res) => {
  const { apartment, floor, flat } = req.body;
  if (!apartment || !floor || !flat) return res.status(400).json({ error: "apartment, floor, flat are required" });

  const existingOwner = await dbGetFlatOwnerByKey(apartment, floor, flat);
  if (existingOwner) return res.status(409).json({ error: "This flat already has an owner account. Delete it first to re-invite." });

  const token = crypto.randomBytes(12).toString("hex");
  await dbCreateFlatInvite(token, apartment, floor, flat);
  res.json({ success: true, token, apartment, floor, flat });
}));

app.get("/api/invites/:token", ah(async (req, res) => {
  const invite = await dbGetFlatInvite(req.params.token);
  if (!invite) return res.status(404).json({ error: "Invalid or expired invite link" });
  if (invite.used) return res.status(410).json({ error: "This invite link has already been used" });
  res.json({ apartment: invite.apartment, floor: invite.floor, flat: invite.flat });
}));

app.post("/api/auth/signup-with-invite", ah(async (req, res) => {
  const { token, name, phone, password } = req.body;
  const invite = await dbGetFlatInvite(token);

  if (!invite) return res.status(404).json({ error: "Invalid or expired invite link" });
  if (invite.used) return res.status(410).json({ error: "This invite link has already been used" });
  if (!name || !phone || !password) return res.status(400).json({ error: "Missing required fields" });
  if (await dbGetUser(phone)) return res.status(409).json({ error: "An account with this phone number already exists" });

  const salt = crypto.randomBytes(16).toString("hex");
  const passwordHash = hashPassword(password, salt);
  await dbCreateUser({ role: "flat_owner", name, phone, salt, passwordHash, apartment: invite.apartment, floor: invite.floor, flat: invite.flat });
  await dbMarkFlatInviteUsed(token);

  const authToken = makeToken();
  await dbCreateSession(authToken, phone);
  res.json({ success: true, token: authToken, user: { role: "flat_owner", name, phone, apartment: invite.apartment, floor: invite.floor, flat: invite.flat } });
}));

// Admin resets a flat owner's password
app.post("/api/users/:apartment/:floor/:flat/reset-password", requireAuth, requireAdmin, ah(async (req, res) => {
  const { apartment, floor, flat } = req.params;
  const owner = await dbGetFlatOwnerByKey(apartment, floor, flat);
  if (!owner) return res.status(404).json({ error: "No account found for this flat" });

  const newPassword = crypto.randomBytes(4).toString("hex");
  const salt = crypto.randomBytes(16).toString("hex");
  await dbUpdateUserPassword(owner.phone, salt, hashPassword(newPassword, salt));
  await dbDeleteSessionsForPhone(owner.phone);

  res.json({ success: true, phone: owner.phone, newPassword });
}));

// Admin deletes a flat owner's account
app.delete("/api/users/:apartment/:floor/:flat", requireAuth, requireAdmin, ah(async (req, res) => {
  const { apartment, floor, flat } = req.params;
  const owner = await dbGetFlatOwnerByKey(apartment, floor, flat);
  if (!owner) return res.status(404).json({ error: "No account found for this flat" });

  await dbDeleteUser(owner.phone);
  await dbDeleteSessionsForPhone(owner.phone);
  res.json({ success: true });
}));

// Login
app.post("/api/auth/login", ah(async (req, res) => {
  const { phone, password } = req.body;
  const user = await dbGetUser(phone);
  if (!user) return res.status(401).json({ error: "Invalid phone number or password" });

  const attemptHash = hashPassword(password, user.salt);
  if (attemptHash !== user.password_hash) return res.status(401).json({ error: "Invalid phone number or password" });

  const token = makeToken();
  await dbCreateSession(token, phone);
  res.json({ success: true, token, user: { role: user.role, name: user.name, phone: user.phone, apartment: user.apartment, floor: user.floor, flat: user.flat } });
}));

app.get("/api/auth/me", requireAuth, (req, res) => { res.json({ user: req.user }); });

// ---------------- HEALTH CHECK ----------------
app.get("/api/health", (req, res) => { res.json({ status: "ok", time: new Date().toISOString() }); });

// ================= ESP32 -> BACKEND (no auth) =================

app.post("/api/device/data/:apartment/:floor/:flat", ah(async (req, res) => {
  const { apartment, floor, flat } = req.params;
  const key = keyFor(apartment, floor, flat);
  const { flow_lpm, total_liters, valve_status } = req.body;

  const record = { flow_lpm, total_liters, valve_status, received_at: new Date().toISOString() };

  await dbUpsertFlatData(key, apartment, floor, flat, record);
  await dbInsertReading(key, record);
  await recordDailyUsage(key, total_liters);
  await checkLimitAlerts(key, flat);

  console.log(`Data from ${key}:`, record);
  res.json({ success: true });
}));

async function recordDailyUsage(key, totalLiters) {
  if (totalLiters == null) return;
  const prev = await dbGetPreviousTotal(key);
  let delta;
  if (prev == null || totalLiters < prev) {
    delta = totalLiters; // first reading, or device rebooted (counter reset)
  } else {
    delta = totalLiters - prev;
  }
  await dbSetPreviousTotal(key, totalLiters);
  if (delta <= 0) return;
  await dbAddDailyUsage(key, todayStr(), delta);
}

async function checkLimitAlerts(key, flat) {
  const limit = await dbGetLimit(key);
  if (!limit) return;
  const todaysUsage = await dbGetDailyUsage(key, todayStr());
  if (!todaysUsage) return;

  let alerts = await dbGetAlertsSent(key);
  if (!alerts || alerts.alert_day !== todayStr()) {
    alerts = { alert_day: todayStr(), level80: false, level100: false };
  }

  const pct = todaysUsage / limit;

  if (pct >= 1 && !alerts.level100) {
    await pushNotification(key, "danger", `Flat ${flat} has reached 100% of today's water limit (${todaysUsage.toFixed(1)}L of ${limit}L).`);
    alerts.level100 = true;
    await dbSetAlertsSent(key, alerts.alert_day, alerts.level80, alerts.level100);
  } else if (pct >= 0.8 && !alerts.level80) {
    await pushNotification(key, "warning", `Flat ${flat} is at ${(pct * 100).toFixed(0)}% of today's water limit (${todaysUsage.toFixed(1)}L of ${limit}L).`);
    alerts.level80 = true;
    await dbSetAlertsSent(key, alerts.alert_day, alerts.level80, alerts.level100);
  }
}

async function pushNotification(key, level, message) {
  await dbInsertNotification(key, level, message);
  console.log(`ALERT [${key}]: ${message}`);
}

app.get("/api/device/command/:apartment/:floor/:flat", (req, res) => {
  const key = keyFor(req.params.apartment, req.params.floor, req.params.flat);
  const command = pendingCommands[key] || null;
  pendingCommands[key] = null;
  res.json({ command });
});

// ================= APP/DASHBOARD -> BACKEND (auth required) =================

app.get("/api/flats", requireAuth, requireAdmin, ah(async (req, res) => {
  const [allFlatsData, owners] = await Promise.all([dbGetAllFlatsData(), dbGetAllFlatOwners()]);

  const keysSet = new Set([
    ...allFlatsData.map((f) => f.key),
    ...owners.map((u) => keyFor(u.apartment, u.floor, u.flat)),
  ]);

  const result = await Promise.all([...keysSet].map(async (key) => {
    const owner = owners.find((u) => keyFor(u.apartment, u.floor, u.flat) === key);
    const flatData = allFlatsData.find((f) => f.key === key);
    const [apartment, floor, flat] = key.split("/");
    const active = await isSubscriptionActive(key);
    const limit = await dbGetLimit(key);
    const todayUsage = active ? await dbGetDailyUsage(key, todayStr()) : null;

    return {
      apartment, floor, flat,
      owner_name: owner ? owner.name : null,
      owner_phone: owner ? owner.phone : null,
      valve_status: flatData ? flatData.valve_status : null,
      subscription_active: active,
      flow_lpm: active ? (flatData ? Number(flatData.flow_lpm) || 0 : 0) : null,
      total_liters: active ? (flatData ? Number(flatData.total_liters) || 0 : 0) : null,
      today_usage: active ? todayUsage : null,
      limit,
    };
  }));

  res.json(result);
}));

app.get("/api/flats/:apartment/:floor/:flat", requireAuth, ah(async (req, res) => {
  const { apartment, floor, flat } = req.params;
  const key = keyFor(apartment, floor, flat);

  if (req.user.role !== "admin" && req.user.role !== "super_admin") {
    const ownKey = keyFor(req.user.apartment, req.user.floor, req.user.flat);
    if (ownKey !== key) return res.status(403).json({ error: "You can only view your own flat" });
  }

  const active = await isSubscriptionActive(key);
  const limit = await dbGetLimit(key);

  res.json({
    subscription_active: active,
    latest: active ? await dbGetFlatData(key) : null,
    history: active ? await dbGetHistory(key) : [],
    limit,
  });
}));

app.post("/api/valve/:apartment/:floor/:flat", requireAuth, ah(async (req, res) => {
  const { apartment, floor, flat } = req.params;
  const { action } = req.body;
  const key = keyFor(apartment, floor, flat);

  if (req.user.role !== "admin" && req.user.role !== "super_admin") {
    const ownKey = keyFor(req.user.apartment, req.user.floor, req.user.flat);
    if (ownKey !== key) return res.status(403).json({ error: "You can only control your own flat's valve" });
  }
  if (!["OPEN", "CLOSE"].includes(action)) return res.status(400).json({ error: "action must be OPEN or CLOSE" });

  const liveSocket = deviceSockets[key];
  const deliveredInstantly = !!(liveSocket && liveSocket.readyState === WebSocket.OPEN);
  if (deliveredInstantly) {
    liveSocket.send(JSON.stringify({ command: action }));
    console.log(`Command sent INSTANTLY via WebSocket to ${key}: ${action}`);
  } else {
    console.log(`Device ${key} not connected via WebSocket — queuing for next poll`);
  }

  pendingCommands[key] = action;
  console.log(`Command queued for ${key}: ${action} (by ${req.user.role} ${req.user.name})`);
  res.json({ success: true, apartment, floor, flat, action, delivered_instantly: deliveredInstantly });
}));

app.get("/api/usage/:apartment/:floor/:flat", requireAuth, ah(async (req, res) => {
  const { apartment, floor, flat } = req.params;
  const key = keyFor(apartment, floor, flat);

  if (req.user.role !== "admin" && req.user.role !== "super_admin") {
    const ownKey = keyFor(req.user.apartment, req.user.floor, req.user.flat);
    if (ownKey !== key) return res.status(403).json({ error: "You can only view your own flat's usage" });
  }

  const active = await isSubscriptionActive(key);
  if (!active) return res.json({ subscription_active: false, days: [], today: 0 });

  const days = await dbGetAllDailyUsage(key);
  const today = await dbGetDailyUsage(key, todayStr());
  res.json({ subscription_active: true, days, today });
}));

// ---------------- PAYMENT (RAZORPAY) ----------------

app.get("/api/config", requireAuth, (req, res) => { res.json({ subscription_amount_paise: SUBSCRIPTION_AMOUNT_PAISE }); });

app.get("/api/subscription/:apartment/:floor/:flat", requireAuth, ah(async (req, res) => {
  const { apartment, floor, flat } = req.params;
  const key = keyFor(apartment, floor, flat);

  if (req.user.role !== "admin" && req.user.role !== "super_admin") {
    const ownKey = keyFor(req.user.apartment, req.user.floor, req.user.flat);
    if (ownKey !== key) return res.status(403).json({ error: "You can only view your own subscription" });
  }

  res.json({
    active: await isSubscriptionActive(key),
    paid_until: await dbGetSubscriptionPaidUntil(key),
    amount_paise: SUBSCRIPTION_AMOUNT_PAISE,
  });
}));

app.post("/api/payment/create-order", requireAuth, ah(async (req, res) => {
  if (req.user.role !== "flat_owner") return res.status(403).json({ error: "Only flat owners can pay their subscription" });
  if (!razorpay) return res.status(500).json({ error: "Payment gateway is not configured yet. Please contact your admin." });

  const key = keyFor(req.user.apartment, req.user.floor, req.user.flat);
  try {
    const order = await razorpay.orders.create({
      amount: SUBSCRIPTION_AMOUNT_PAISE, currency: "INR",
      receipt: `sub_${key.replace(/\//g, "_")}_${Date.now()}`,
      notes: { apartment: req.user.apartment, floor: req.user.floor, flat: req.user.flat },
    });
    res.json({ order_id: order.id, amount: order.amount, currency: order.currency, key_id: RAZORPAY_KEY_ID });
  } catch (e) {
    console.error("Razorpay order creation failed:", e);
    res.status(500).json({ error: "Could not create payment order" });
  }
}));

app.post("/api/payment/admin/create-order", requireAuth, requireAdmin, ah(async (req, res) => {
  if (!razorpay) return res.status(500).json({ error: "Payment gateway is not configured yet." });

  const { flats } = req.body;
  if (!Array.isArray(flats) || flats.length === 0) return res.status(400).json({ error: "Select at least one flat to pay for" });

  const keys = flats.map((f) => keyFor(f.apartment, f.floor, f.flat));
  const amount = SUBSCRIPTION_AMOUNT_PAISE * keys.length;

  try {
    const order = await razorpay.orders.create({
      amount, currency: "INR", receipt: `admin_bulk_${Date.now()}`, notes: { flats: keys.join(",") },
    });
    await dbSaveAdminOrder(order.id, keys);
    res.json({ order_id: order.id, amount: order.amount, currency: order.currency, key_id: RAZORPAY_KEY_ID, flat_count: keys.length });
  } catch (e) {
    console.error("Razorpay admin order creation failed:", e);
    res.status(500).json({ error: "Could not create payment order" });
  }
}));

app.post("/api/payment/admin/verify", requireAuth, requireAdmin, ah(async (req, res) => {
  const { razorpay_order_id, razorpay_payment_id, razorpay_signature } = req.body;
  if (!razorpay_order_id || !razorpay_payment_id || !razorpay_signature) return res.status(400).json({ error: "Missing payment verification fields" });

  const expectedSignature = crypto.createHmac("sha256", RAZORPAY_KEY_SECRET).update(`${razorpay_order_id}|${razorpay_payment_id}`).digest("hex");
  if (expectedSignature !== razorpay_signature) return res.status(400).json({ error: "Payment verification failed" });

  const keys = await dbGetAdminOrder(razorpay_order_id);
  if (!keys) return res.status(404).json({ error: "Order not found" });

  const paidUntil = new Date(Date.now() + SUBSCRIPTION_DAYS * 24 * 60 * 60 * 1000).toISOString();
  for (const key of keys) {
    await dbSetSubscription(key, paidUntil);
    console.log(`Subscription activated (by admin) for ${key} until ${paidUntil}`);
  }
  await dbDeleteAdminOrder(razorpay_order_id);

  res.json({ success: true, flats_paid: keys, paid_until: paidUntil });
}));

app.post("/api/payment/verify", requireAuth, ah(async (req, res) => {
  if (req.user.role !== "flat_owner") return res.status(403).json({ error: "Only flat owners can pay their subscription" });

  const { razorpay_order_id, razorpay_payment_id, razorpay_signature } = req.body;
  if (!razorpay_order_id || !razorpay_payment_id || !razorpay_signature) return res.status(400).json({ error: "Missing payment verification fields" });

  const expectedSignature = crypto.createHmac("sha256", RAZORPAY_KEY_SECRET).update(`${razorpay_order_id}|${razorpay_payment_id}`).digest("hex");
  if (expectedSignature !== razorpay_signature) return res.status(400).json({ error: "Payment verification failed" });

  const key = keyFor(req.user.apartment, req.user.floor, req.user.flat);
  const paidUntil = new Date(Date.now() + SUBSCRIPTION_DAYS * 24 * 60 * 60 * 1000).toISOString();
  await dbSetSubscription(key, paidUntil);

  console.log(`Subscription activated for ${key} until ${paidUntil}`);
  res.json({ success: true, paid_until: paidUntil });
}));

app.post("/api/limits/:apartment/:floor/:flat", requireAuth, requireAdmin, ah(async (req, res) => {
  const { apartment, floor, flat } = req.params;
  const { daily_limit_liters } = req.body;
  const key = keyFor(apartment, floor, flat);
  await dbSetLimit(key, daily_limit_liters);
  await dbSetAlertsSent(key, todayStr(), false, false);
  res.json({ success: true, key, daily_limit_liters });
}));

app.get("/api/notifications", requireAuth, ah(async (req, res) => {
  if (req.user.role === "admin" || req.user.role === "super_admin") {
    return res.json(await dbGetAdminNotifications());
  }
  const key = keyFor(req.user.apartment, req.user.floor, req.user.flat);
  res.json(await dbGetFlatNotifications(key));
}));

// ---------------- START SERVER ----------------
server.listen(PORT, () => {
  console.log(`Backend (HTTP + WebSocket + Auth + Supabase) running on port ${PORT}`);
});
