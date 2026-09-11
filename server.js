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
const CYCLE_DAYS = 30;
const TRIAL_DAYS = 7;
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const EXPIRY_ALERT_DAYS = 3;

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
    managed_by: u.managedBy || null,
  });
  if (error) throw error;
}
async function dbGetAllAdmins() {
  const { data } = await supabase.from("users").select("*").eq("role", "admin");
  return data || [];
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

async function dbCreateFlatInvite(token, apartment, floor, flat, createdBy) {
  await supabase.from("flat_invites").insert({ token, apartment, floor, flat, created_by: createdBy || null });
}
async function dbGetFlatInvite(token) {
  const { data } = await supabase.from("flat_invites").select("*").eq("token", token).maybeSingle();
  return data;
}
async function dbGetUnusedFlatInvites() {
  const { data } = await supabase.from("flat_invites").select("*").eq("used", false);
  return data || [];
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

async function dbGetSetting(key, fallback) {
  const { data } = await supabase.from("app_settings").select("value").eq("key", key).maybeSingle();
  if (!data) return fallback;
  const n = parseFloat(data.value);
  return isNaN(n) ? fallback : n;
}
async function dbSetSetting(key, value) { await supabase.from("app_settings").upsert({ key, value: String(value) }); }
async function getTrialDays() { return await dbGetSetting("trial_days", TRIAL_DAYS); }

const CYCLE_MS = CYCLE_DAYS * MS_PER_DAY;

// The "reference" is the anchor point for this flat's billing cycle:
// - First ever cycle anchor is N days after signup (end of free trial, N configurable by super admin)
// - After any payment, the anchor moves forward by exactly the cycles paid for —
//   never reset to "now", so late payments don't lose or gain days.
async function getBillingReference(key) {
  const [apartment, floor, flat] = key.split("/");
  const owner = await dbGetFlatOwnerByKey(apartment, floor, flat);
  const trialDays = await getTrialDays();
  const trialEnd = owner ? new Date(new Date(owner.created_at).getTime() + trialDays * MS_PER_DAY) : new Date();

  const paidUntilISO = await dbGetSubscriptionPaidUntil(key);
  const paidUntil = paidUntilISO ? new Date(paidUntilISO) : null;

  // The reference never moves backward — it's whichever is later: the trial end,
  // or the last date they're paid up to. All future 30-day cycles count from here.
  const reference = paidUntil && paidUntil > trialEnd ? paidUntil : trialEnd;
  return { reference, trialEnd };
}

async function isSubscriptionActive(key) {
  const { reference } = await getBillingReference(key);
  return new Date() < reference;
}

// How much is owed right now (in 30-day cycles), and what the new expiry will be once paid.
// Late payment never resets the cycle clock — arrears just accumulate in fixed 30-day blocks
// from the reference date, so a late payment only unlocks the remainder of that block.
async function computeAmountDue(key) {
  const { reference } = await getBillingReference(key);
  const now = new Date();

  let cycles;
  if (now <= reference) {
    cycles = 1; // paying ahead of time / renewing right on schedule — one normal cycle
  } else {
    cycles = Math.ceil((now - reference) / CYCLE_MS); // overdue cycles, including the current one
  }

  const amountDue = SUBSCRIPTION_AMOUNT_PAISE * cycles;
  const newPaidUntil = new Date(reference.getTime() + cycles * CYCLE_MS);
  return { cycles, amountDue, newPaidUntil };
}

// Sends a "your subscription is about to expire" notification once per billing reference,
// starting 3 days before it lapses. Safe to call repeatedly — it only fires once per reference.
async function checkExpiryAlert(key) {
  const { reference } = await getBillingReference(key);
  const now = new Date();
  const msLeft = reference - now;
  if (msLeft <= 0 || msLeft > 3 * MS_PER_DAY) return;

  const alertedFor = await dbGetSubAlertedDue(key);
  if (alertedFor === reference.toISOString()) return; // already alerted for this exact reference

  const daysLeft = Math.max(1, Math.ceil(msLeft / MS_PER_DAY));
  const [, , flat] = key.split("/");
  await pushNotification(key, "warning", `Flat ${flat}'s water usage subscription expires in ${daysLeft} day${daysLeft > 1 ? "s" : ""}. Please renew to avoid losing access.`);
  await dbSetSubAlertedDue(key, reference.toISOString());
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

async function dbGetHasValve(key) {
  const { data } = await supabase.from("flat_config").select("has_valve").eq("key", key).maybeSingle();
  return data ? data.has_valve : true; // default: assume valve installed unless told otherwise
}
async function dbSetHasValve(key, hasValve) { await supabase.from("flat_config").upsert({ key, has_valve: hasValve }); }

const DEFAULT_PULSES_PER_LITER = 160;
async function dbGetCalibration(key) {
  const { data } = await supabase.from("flat_config").select("pulses_per_liter").eq("key", key).maybeSingle();
  return data && data.pulses_per_liter ? Number(data.pulses_per_liter) : DEFAULT_PULSES_PER_LITER;
}
async function dbSetCalibration(key, pulsesPerLiter) { await supabase.from("flat_config").upsert({ key, pulses_per_liter: pulsesPerLiter }); }

async function dbSavePaymentOrder(orderId, key, periods, baseDueISO) {
  await supabase.from("payment_orders").insert({ order_id: orderId, key, periods, base_due: baseDueISO });
}
async function dbGetPaymentOrder(orderId) {
  const { data } = await supabase.from("payment_orders").select("*").eq("order_id", orderId).maybeSingle();
  return data;
}
async function dbDeletePaymentOrder(orderId) { await supabase.from("payment_orders").delete().eq("order_id", orderId); }

async function dbGetSubAlertedDue(key) {
  const { data } = await supabase.from("sub_alert_sent").select("alerted_for_due").eq("key", key).maybeSingle();
  return data ? data.alerted_for_due : null;
}
async function dbSetSubAlertedDue(key, dueISO) { await supabase.from("sub_alert_sent").upsert({ key, alerted_for_due: dueISO }); }

// ---------------- AUTH MIDDLEWARE ----------------
async function requireAuth(req, res, next) {
  try {
    const authHeader = req.headers.authorization || "";
    const token = authHeader.replace("Bearer ", "");
    const phone = await dbGetSessionPhone(token);
    if (!phone) return res.status(401).json({ error: "Not authenticated" });
    const user = await dbGetUser(phone);
    if (!user) return res.status(401).json({ error: "Not authenticated" });
    req.user = { role: user.role, name: user.name, phone: user.phone, apartment: user.apartment, floor: user.floor, flat: user.flat, profile_pic: user.profile_pic || null };
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
  const { token, name, phone, password, apartment } = req.body;
  const invite = await dbGetAdminInvite(token);

  if (!invite) return res.status(404).json({ error: "Invalid or expired invite link" });
  if (invite.used) return res.status(410).json({ error: "This invite link has already been used" });
  if (!name || !phone || !password || !apartment) return res.status(400).json({ error: "Missing required fields" });
  if (await dbGetUser(phone)) return res.status(409).json({ error: "An account with this phone number already exists" });

  const salt = crypto.randomBytes(16).toString("hex");
  const passwordHash = hashPassword(password, salt);
  await dbCreateUser({ role: "admin", name, phone, salt, passwordHash, apartment });
  await dbMarkAdminInviteUsed(token);

  const authToken = makeToken();
  await dbCreateSession(authToken, phone);
  res.json({ success: true, token: authToken, user: { role: "admin", name, phone, apartment, floor: null, flat: null } });
}));

// ---------------- INVITE-BASED FLAT OWNER SIGNUP ----------------

app.post("/api/invites", requireAuth, requireAdmin, ah(async (req, res) => {
  const { apartment, floor, flat } = req.body;
  if (!apartment || !floor || !flat) return res.status(400).json({ error: "apartment, floor, flat are required" });

  const existingOwner = await dbGetFlatOwnerByKey(apartment, floor, flat);
  if (existingOwner) return res.status(409).json({ error: "This flat already has an owner account. Delete it first to re-invite." });

  const token = crypto.randomBytes(12).toString("hex");
  await dbCreateFlatInvite(token, apartment, floor, flat, req.user.phone);
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
  await dbCreateUser({ role: "flat_owner", name, phone, salt, passwordHash, apartment: invite.apartment, floor: invite.floor, flat: invite.flat, managedBy: invite.created_by });
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
  const key = keyFor(apartment, floor, flat);
  const owner = await dbGetFlatOwnerByKey(apartment, floor, flat);

  if (owner) {
    await dbDeleteUser(owner.phone);
    await dbDeleteSessionsForPhone(owner.phone);
  }

  // Purge all stored data for this flat key so it fully disappears from the dashboard,
  // whether or not anyone had actually signed up yet.
  await supabase.from("flats_data").delete().eq("key", key);
  await supabase.from("readings_history").delete().eq("key", key);
  await supabase.from("daily_usage").delete().eq("key", key);
  await supabase.from("subscriptions").delete().eq("key", key);
  await supabase.from("limits").delete().eq("key", key);
  await supabase.from("flat_config").delete().eq("key", key);
  await supabase.from("notifications").delete().eq("key", key);
  await supabase.from("previous_total").delete().eq("key", key);
  await supabase.from("alerts_sent").delete().eq("key", key);
  await supabase.from("sub_alert_sent").delete().eq("key", key);
  await supabase.from("flat_invites").delete().eq("apartment", apartment).eq("floor", floor).eq("flat", flat).eq("used", false);

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
  res.json({ success: true, token, user: { role: user.role, name: user.name, phone: user.phone, apartment: user.apartment, floor: user.floor, flat: user.flat, profile_pic: user.profile_pic || null } });
}));

app.get("/api/auth/me", requireAuth, (req, res) => { res.json({ user: req.user }); });

// Any logged-in user (flat owner, admin, super admin) can set their own profile picture.
// Expects a small base64 data URL (client should resize/compress before sending).
app.post("/api/profile/photo", requireAuth, ah(async (req, res) => {
  const { photo } = req.body;
  if (!photo || typeof photo !== "string" || !photo.startsWith("data:image/")) {
    return res.status(400).json({ error: "Please send a valid image." });
  }
  if (photo.length > 700000) return res.status(400).json({ error: "Image is too large. Please choose a smaller photo." });

  await supabase.from("users").update({ profile_pic: photo }).eq("phone", req.user.phone);
  res.json({ success: true });
}));

app.delete("/api/profile/photo", requireAuth, ah(async (req, res) => {
  await supabase.from("users").update({ profile_pic: null }).eq("phone", req.user.phone);
  res.json({ success: true });
}));

// ---------------- HEALTH CHECK ----------------
app.get("/api/health", (req, res) => { res.json({ status: "ok", time: new Date().toISOString() }); });

// ================= ESP32 -> BACKEND (no auth) =================

app.post("/api/device/data/:apartment/:floor/:flat", ah(async (req, res) => {
  const { apartment, floor, flat } = req.params;
  const key = keyFor(apartment, floor, flat);
  const { pulses, interval_seconds, valve_status } = req.body;

  const pulsesPerLiter = await dbGetCalibration(key);
  const intervalSec = interval_seconds || 30;
  const litersSinceLast = (Number(pulses) || 0) / pulsesPerLiter;
  const flow_lpm = litersSinceLast / (intervalSec / 60);

  const existing = await dbGetFlatData(key);
  const total_liters = (existing && existing.total_liters ? Number(existing.total_liters) : 0) + litersSinceLast;

  const record = { flow_lpm, total_liters, valve_status, received_at: new Date().toISOString() };

  await dbUpsertFlatData(key, apartment, floor, flat, record);
  await dbInsertReading(key, record);
  await recordDailyUsage(key, total_liters);
  await checkLimitAlerts(key, flat);

  console.log(`Data from ${key}: pulses=${pulses} (${pulsesPerLiter}/L) -> ${record.flow_lpm.toFixed(2)} L/min, total ${record.total_liters.toFixed(2)}L`);
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

// Admin toggles whether a flat has a valve fitted (flow-only flats hide valve controls)
app.post("/api/flat-config/:apartment/:floor/:flat", requireAuth, requireAdmin, ah(async (req, res) => {
  const { apartment, floor, flat } = req.params;
  const { has_valve, pulses_per_liter } = req.body;
  const key = keyFor(apartment, floor, flat);

  if (has_valve !== undefined) await dbSetHasValve(key, !!has_valve);
  if (pulses_per_liter !== undefined) {
    const n = parseFloat(pulses_per_liter);
    if (!n || n <= 0) return res.status(400).json({ error: "Enter a valid pulses-per-liter number" });
    await dbSetCalibration(key, n);
  }

  res.json({ success: true, key });
}));

app.get("/api/flats", requireAuth, requireAdmin, ah(async (req, res) => {
  // Which admin's flats to show: a regular admin always sees only their own;
  // a super admin sees everything by default, a specific admin's flats via ?admin_phone=,
  // or orphaned/unassigned flats (e.g. left behind after an admin was deleted) via ?admin_phone=__unassigned__
  const scopeToAdmin = req.user.role === "admin" ? req.user.phone : (req.query.admin_phone || null);
  const showUnassigned = scopeToAdmin === "__unassigned__";

  const [allFlatsData, owners, pendingInvites, allAdmins] = await Promise.all([
    dbGetAllFlatsData(), dbGetAllFlatOwners(), dbGetUnusedFlatInvites(), dbGetAllAdmins(),
  ]);
  const adminPhones = new Set(allAdmins.map((a) => a.phone));

  let scopedOwners, scopedInvites;
  if (showUnassigned) {
    scopedOwners = owners.filter((u) => !u.managed_by || !adminPhones.has(u.managed_by));
    scopedInvites = pendingInvites.filter((i) => !i.created_by || !adminPhones.has(i.created_by));
  } else {
    scopedOwners = scopeToAdmin ? owners.filter((u) => u.managed_by === scopeToAdmin) : owners;
    scopedInvites = scopeToAdmin ? pendingInvites.filter((i) => i.created_by === scopeToAdmin) : pendingInvites;
  }
  const ownerKeys = new Set(owners.map((u) => keyFor(u.apartment, u.floor, u.flat)));

  const keysSet = new Set([
    ...scopedOwners.map((u) => keyFor(u.apartment, u.floor, u.flat)),
    ...scopedInvites.map((i) => keyFor(i.apartment, i.floor, i.flat)),
    // include raw device data only when unscoped (super admin, no filter) since we can't attribute it to an admin otherwise
    ...(scopeToAdmin ? [] : allFlatsData.filter((f) => !ownerKeys.has(f.key)).map((f) => f.key)),
  ]);

  const result = await Promise.all([...keysSet].map(async (key) => {
    const owner = owners.find((u) => keyFor(u.apartment, u.floor, u.flat) === key);
    const flatData = allFlatsData.find((f) => f.key === key);
    const [apartment, floor, flat] = key.split("/");
    const active = await isSubscriptionActive(key);
    const limit = await dbGetLimit(key);
    const todayUsage = active ? await dbGetDailyUsage(key, todayStr()) : null;
    const hasValve = await dbGetHasValve(key);
    const calibration = await dbGetCalibration(key);

    return {
      apartment, floor, flat,
      owner_name: owner ? owner.name : null,
      owner_pic: owner ? owner.profile_pic : null,
      managed_by: owner ? owner.managed_by : null,
      owner_phone: owner ? owner.phone : null,
      valve_status: flatData ? flatData.valve_status : null,
      has_valve: hasValve,
      pulses_per_liter: calibration,
      subscription_active: active,
      flow_lpm: active ? (flatData ? Number(flatData.flow_lpm) || 0 : 0) : null,
      total_liters: active ? (flatData ? Number(flatData.total_liters) || 0 : 0) : null,
      today_usage: active ? todayUsage : null,
      limit,
    };
  }));

  res.json(result);
}));

// Super admin: list all apartment admins, with a quick flat-count summary for each
// Super admin deletes an apartment admin's account
app.delete("/api/admins/:phone", requireAuth, requireSuperAdmin, ah(async (req, res) => {
  const { phone } = req.params;
  const target = await dbGetUser(phone);
  if (!target || target.role !== "admin") return res.status(404).json({ error: "Admin not found" });

  await dbDeleteUser(phone);
  await dbDeleteSessionsForPhone(phone);
  res.json({ success: true });
}));

// Admin sets (or updates) their apartment's building layout template
app.post("/api/building-layout", requireAuth, requireAdmin, ah(async (req, res) => {
  const { floors, flats_per_floor } = req.body;
  const f = parseInt(floors, 10), fpf = parseInt(flats_per_floor, 10);
  if (!f || f < 1 || !fpf || fpf < 1) return res.status(400).json({ error: "Enter valid floors and flats-per-floor numbers" });

  await supabase.from("building_layout").upsert({ admin_phone: req.user.phone, floors: f, flats_per_floor: fpf });
  res.json({ success: true, floors: f, flats_per_floor: fpf });
}));

// Get a building layout — admin's own, or (for super admin) a specific admin's via ?admin_phone=
app.get("/api/building-layout", requireAuth, requireAdmin, ah(async (req, res) => {
  const targetPhone = req.user.role === "super_admin" ? (req.query.admin_phone || req.user.phone) : req.user.phone;
  const { data } = await supabase.from("building_layout").select("*").eq("admin_phone", targetPhone).maybeSingle();
  res.json(data || { floors: 0, flats_per_floor: 0 });
}));

app.get("/api/admins", requireAuth, requireSuperAdmin, ah(async (req, res) => {
  const [admins, owners] = await Promise.all([dbGetAllAdmins(), dbGetAllFlatOwners()]);

  const result = admins.map((a) => {
    const theirFlats = owners.filter((u) => u.managed_by === a.phone);
    return {
      name: a.name, phone: a.phone, apartment: a.apartment, profile_pic: a.profile_pic, created_at: a.created_at,
      total_flats: theirFlats.length,
    };
  });

  res.json(result);
}));

// Super admin assigns/reassigns which apartment admin manages a given flat owner
app.post("/api/flats/:apartment/:floor/:flat/assign-admin", requireAuth, requireSuperAdmin, ah(async (req, res) => {
  const { apartment, floor, flat } = req.params;
  const { admin_phone } = req.body; // null/empty to unassign
  const owner = await dbGetFlatOwnerByKey(apartment, floor, flat);
  if (!owner) return res.status(404).json({ error: "No signed-up owner found for this flat" });

  if (admin_phone) {
    const target = await dbGetUser(admin_phone);
    if (!target || target.role !== "admin") return res.status(400).json({ error: "Target admin not found" });
  }

  await supabase.from("users").update({ managed_by: admin_phone || null }).eq("phone", owner.phone);
  res.json({ success: true });
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
  const hasValve = await dbGetHasValve(key);
  const { reference, trialEnd } = await getBillingReference(key);
  const now = new Date();
  const isTrial = now < trialEnd;
  const daysRemaining = Math.max(0, Math.ceil((reference.getTime() - now.getTime()) / MS_PER_DAY));

  res.json({
    subscription_active: active,
    has_valve: hasValve,
    latest: active ? await dbGetFlatData(key) : null,
    history: active ? await dbGetHistory(key) : [],
    limit,
    subscription: {
      is_trial: isTrial,
      valid_until: reference.toISOString(),
      days_remaining: daysRemaining,
    },
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

app.get("/api/config", requireAuth, ah(async (req, res) => {
  res.json({
    subscription_amount_paise: SUBSCRIPTION_AMOUNT_PAISE,
    trial_days: await getTrialDays(),
    support_contact: (await supabase.from("app_settings").select("value").eq("key", "support_contact").maybeSingle()).data?.value || "",
  });
}));

app.post("/api/settings/support-contact", requireAuth, requireSuperAdmin, ah(async (req, res) => {
  const { support_contact } = req.body;
  if (!support_contact || !support_contact.trim()) return res.status(400).json({ error: "Enter a valid contact number" });
  await dbSetSetting("support_contact", support_contact.trim());
  res.json({ success: true, support_contact: support_contact.trim() });
}));

app.post("/api/settings/trial-days", requireAuth, requireSuperAdmin, ah(async (req, res) => {
  const days = parseInt(req.body.trial_days, 10);
  if (!days || days < 0) return res.status(400).json({ error: "Enter a valid number of days" });
  await dbSetSetting("trial_days", days);
  res.json({ success: true, trial_days: days });
}));

app.get("/api/subscription/:apartment/:floor/:flat", requireAuth, ah(async (req, res) => {
  const { apartment, floor, flat } = req.params;
  const key = keyFor(apartment, floor, flat);

  if (req.user.role !== "admin" && req.user.role !== "super_admin") {
    const ownKey = keyFor(req.user.apartment, req.user.floor, req.user.flat);
    if (ownKey !== key) return res.status(403).json({ error: "You can only view your own subscription" });
  }

  await checkExpiryAlert(key);
  const { cycles, amountDue } = await computeAmountDue(key);

  res.json({
    active: await isSubscriptionActive(key),
    paid_until: await dbGetSubscriptionPaidUntil(key),
    amount_paise: amountDue,
    cycles_due: cycles,
  });
}));

app.post("/api/payment/create-order", requireAuth, ah(async (req, res) => {
  if (req.user.role !== "flat_owner") return res.status(403).json({ error: "Only flat owners can pay their subscription" });
  if (!razorpay) return res.status(500).json({ error: "Payment gateway is not configured yet. Please contact your admin." });

  const key = keyFor(req.user.apartment, req.user.floor, req.user.flat);
  const { cycles, amountDue } = await computeAmountDue(key);
  const { reference } = await getBillingReference(key);

  try {
    const order = await razorpay.orders.create({
      amount: amountDue, currency: "INR",
      receipt: `sub_${key.replace(/\//g, "_")}_${Date.now()}`,
      notes: { apartment: req.user.apartment, floor: req.user.floor, flat: req.user.flat, cycles: String(cycles) },
    });
    await dbSavePaymentOrder(order.id, key, cycles, reference.toISOString());
    res.json({ order_id: order.id, amount: order.amount, currency: order.currency, key_id: RAZORPAY_KEY_ID, cycles });
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
  const entries = [];
  let totalAmount = 0;

  for (const key of keys) {
    const { cycles, amountDue } = await computeAmountDue(key);
    const { reference } = await getBillingReference(key);
    entries.push({ key, cycles, base_due: reference.toISOString() });
    totalAmount += amountDue;
  }

  try {
    const order = await razorpay.orders.create({
      amount: totalAmount, currency: "INR", receipt: `admin_bulk_${Date.now()}`, notes: { flats: keys.join(",") },
    });
    await dbSaveAdminOrder(order.id, entries);
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

  const entries = await dbGetAdminOrder(razorpay_order_id);
  if (!entries) return res.status(404).json({ error: "Order not found" });

  const paidKeys = [];
  for (const entry of entries) {
    const newPaidUntil = new Date(new Date(entry.base_due).getTime() + entry.cycles * CYCLE_MS).toISOString();
    await dbSetSubscription(entry.key, newPaidUntil);
    paidKeys.push(entry.key);
    console.log(`Subscription extended (by admin) for ${entry.key} by ${entry.cycles} cycle(s), now valid until ${newPaidUntil}`);
  }
  await dbDeleteAdminOrder(razorpay_order_id);

  res.json({ success: true, flats_paid: paidKeys });
}));

app.post("/api/payment/verify", requireAuth, ah(async (req, res) => {
  if (req.user.role !== "flat_owner") return res.status(403).json({ error: "Only flat owners can pay their subscription" });

  const { razorpay_order_id, razorpay_payment_id, razorpay_signature } = req.body;
  if (!razorpay_order_id || !razorpay_payment_id || !razorpay_signature) return res.status(400).json({ error: "Missing payment verification fields" });

  const expectedSignature = crypto.createHmac("sha256", RAZORPAY_KEY_SECRET).update(`${razorpay_order_id}|${razorpay_payment_id}`).digest("hex");
  if (expectedSignature !== razorpay_signature) return res.status(400).json({ error: "Payment verification failed" });

  const order = await dbGetPaymentOrder(razorpay_order_id);
  if (!order) return res.status(404).json({ error: "Order not found or already used" });

  const newPaidUntil = new Date(new Date(order.base_due).getTime() + order.periods * CYCLE_MS).toISOString();
  await dbSetSubscription(order.key, newPaidUntil);
  await dbDeletePaymentOrder(razorpay_order_id);

  console.log(`Subscription extended for ${order.key} by ${order.periods} cycle(s), now valid until ${newPaidUntil}`);
  res.json({ success: true, paid_until: newPaidUntil, cycles_paid: order.periods });
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
