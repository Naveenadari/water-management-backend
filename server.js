/* ==========================================================
   WATER MANAGEMENT SYSTEM - BACKEND SERVER
   (HTTP + WebSocket + Auth + Razorpay + Supabase persistence)
   ========================================================== */

const express = require("express");
const cors = require("cors");
const crypto = require("crypto");
const admin = require("firebase-admin");
const http = require("http");
const WebSocket = require("ws");
const Razorpay = require("razorpay");
const { createClient } = require("@supabase/supabase-js");

const app = express();
app.use(cors());
app.use(express.json({ verify: (req, res, buf) => { req.rawBody = buf; } }));
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
const DEFAULT_SUBSCRIPTION_AMOUNT_PAISE = parseInt(process.env.SUBSCRIPTION_AMOUNT_PAISE || "9900", 10);
const CYCLE_DAYS = 30;
const TRIAL_DAYS = 7;
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const EXPIRY_ALERT_DAYS = 3;

// ESP32 sends a reading every ~30s; treat a sensor as "offline" once it hasn't
// been heard from in 3x that interval (covers a missed send or two without false alarms).
const SENSOR_OFFLINE_MS = 90 * 1000;
function sensorStatusFor(receivedAtIso) {
  if (!receivedAtIso) return { sensor_status: "never_connected", last_seen: null };
  const lastSeenMs = new Date(receivedAtIso).getTime();
  const isOnline = (Date.now() - lastSeenMs) <= SENSOR_OFFLINE_MS;
  return { sensor_status: isOnline ? "online" : "offline", last_seen: receivedAtIso };
}

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
async function getSubscriptionAmount() { return Math.round(await dbGetSetting("subscription_amount_paise", DEFAULT_SUBSCRIPTION_AMOUNT_PAISE)); }

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

  const perCycleAmount = await getSubscriptionAmount();
  const amountDue = perCycleAmount * cycles;
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

// ---------------- OTP (via MSG91) — login OTP + forgot-password OTP ----------------
// ---------------- FIREBASE ADMIN (phone auth verification) ----------------
let firebaseAdminReady = false;
try {
  if (process.env.FIREBASE_SERVICE_ACCOUNT_JSON) {
    const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_JSON);
    admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
    firebaseAdminReady = true;
    console.log("Firebase Admin initialized.");
  } else {
    console.log("FIREBASE_SERVICE_ACCOUNT_JSON not set — Firebase phone login disabled.");
  }
} catch (e) {
  console.error("Firebase Admin init failed:", e.message);
}

// Verifies a Firebase phone-auth ID token (issued client-side after the user completes
// the OTP flow directly with Google) and returns the verified phone number, stripped of
// the "+91" country code to match how phone numbers are stored elsewhere in this app.
async function verifyFirebaseIdToken(idToken) {
  if (!firebaseAdminReady) throw new Error("Firebase phone login is not configured on the server yet.");
  const decoded = await admin.auth().verifyIdToken(idToken);
  if (!decoded.phone_number) throw new Error("This login method did not return a verified phone number.");
  return decoded.phone_number.replace(/^\+91/, "");
}

app.post("/api/auth/firebase-login", ah(async (req, res) => {
  const { id_token, purpose } = req.body;
  if (!id_token || !["login", "reset"].includes(purpose)) return res.status(400).json({ error: "Invalid request" });

  let phone;
  try {
    phone = await verifyFirebaseIdToken(id_token);
  } catch (e) {
    return res.status(401).json({ error: e.message || "Phone verification failed" });
  }

  const user = await dbGetUser(phone);
  if (!user) return res.status(404).json({ error: "No account found with this phone number" });

  if (purpose === "login") {
    const token = makeToken();
    await dbCreateSession(token, phone);
    return res.json({ success: true, token, user: { role: user.role, name: user.name, phone: user.phone, apartment: user.apartment, floor: user.floor, flat: user.flat, profile_pic: user.profile_pic || null } });
  }

  // purpose === 'reset'
  const resetToken = crypto.randomBytes(24).toString("hex");
  const tokenExpiry = new Date(Date.now() + 10 * 60 * 1000).toISOString();
  await supabase.from("password_reset_tokens").insert({ token: resetToken, phone, expires_at: tokenExpiry });
  res.json({ success: true, reset_token: resetToken, phone });
}));

const MSG91_AUTH_KEY = process.env.MSG91_AUTH_KEY;
const MSG91_TEMPLATE_ID = process.env.MSG91_TEMPLATE_ID; // set once the DLT-approved OTP template is ready
const OTP_EXPIRY_SECONDS = 60;

async function sendOtpSms(phone, otp) {
  // Until MSG91_TEMPLATE_ID is configured (DLT approval pending), OTPs are just logged
  // server-side so testing can continue without real SMS delivery.
  if (!MSG91_AUTH_KEY || !MSG91_TEMPLATE_ID) {
    console.log(`[OTP - SMS NOT SENT, no template configured yet] ${phone} -> ${otp}`);
    return { sent: false, reason: "SMS template not configured yet" };
  }
  try {
    const mobile = phone.length === 10 ? "91" + phone : phone;
    const url = `https://control.msg91.com/api/v5/otp?template_id=${MSG91_TEMPLATE_ID}&mobile=${mobile}&authkey=${MSG91_AUTH_KEY}&otp=${otp}&otp_expiry=1`;
    const resp = await fetch(url, { method: "POST" });
    const data = await resp.json();
    console.log("MSG91 response:", data);
    return { sent: data.type === "success", raw: data };
  } catch (e) {
    console.error("MSG91 send failed:", e);
    return { sent: false, reason: "MSG91 request failed" };
  }
}

function generateOtp() { return String(Math.floor(100000 + Math.random() * 900000)); }

// purpose: 'login' or 'reset'
app.post("/api/auth/send-otp", ah(async (req, res) => {
  const { phone, purpose } = req.body;
  if (!phone || !["login", "reset"].includes(purpose)) return res.status(400).json({ error: "Invalid request" });

  const user = await dbGetUser(phone);
  if (!user) return res.status(404).json({ error: "No account found with this phone number" });

  const otp = generateOtp();
  const expiresAt = new Date(Date.now() + OTP_EXPIRY_SECONDS * 1000).toISOString();
  await supabase.from("otps").insert({ phone, otp, purpose, expires_at: expiresAt });

  const result = await sendOtpSms(phone, otp);
  res.json({ success: true, sms_sent: result.sent, message: result.sent ? "OTP sent to your phone." : "OTP generated (SMS delivery not yet configured — check server logs)." });
}));

app.post("/api/auth/verify-otp", ah(async (req, res) => {
  const { phone, otp, purpose } = req.body;
  if (!phone || !otp || !["login", "reset"].includes(purpose)) return res.status(400).json({ error: "Invalid request" });

  const { data: row } = await supabase.from("otps").select("*")
    .eq("phone", phone).eq("otp", otp).eq("purpose", purpose).eq("used", false)
    .gte("expires_at", new Date().toISOString())
    .order("created_at", { ascending: false }).limit(1).maybeSingle();

  if (!row) return res.status(400).json({ error: "Invalid or expired OTP" });
  await supabase.from("otps").update({ used: true }).eq("id", row.id);

  if (purpose === "login") {
    const user = await dbGetUser(phone);
    if (!user) return res.status(404).json({ error: "Account not found" });
    const token = makeToken();
    await dbCreateSession(token, phone);
    return res.json({ success: true, token, user: { role: user.role, name: user.name, phone: user.phone, apartment: user.apartment, floor: user.floor, flat: user.flat, profile_pic: user.profile_pic || null } });
  }

  // purpose === 'reset': issue a short-lived token to allow setting a new password next
  const resetToken = crypto.randomBytes(24).toString("hex");
  const tokenExpiry = new Date(Date.now() + 10 * 60 * 1000).toISOString();
  await supabase.from("password_reset_tokens").insert({ token: resetToken, phone, expires_at: tokenExpiry });
  res.json({ success: true, reset_token: resetToken });
}));

app.post("/api/auth/reset-password", ah(async (req, res) => {
  const { phone, reset_token, new_password } = req.body;
  if (!phone || !reset_token || !new_password) return res.status(400).json({ error: "Missing required fields" });
  if (new_password.length < 6) return res.status(400).json({ error: "Password must be at least 6 characters" });

  const { data: row } = await supabase.from("password_reset_tokens").select("*")
    .eq("token", reset_token).eq("phone", phone).gte("expires_at", new Date().toISOString()).maybeSingle();
  if (!row) return res.status(400).json({ error: "Reset link expired. Please request a new OTP." });

  const user = await dbGetUser(phone);
  if (!user) return res.status(404).json({ error: "Account not found" });

  const passwordHash = hashPassword(new_password, user.salt);
  await supabase.from("users").update({ password_hash: passwordHash }).eq("phone", phone);
  await supabase.from("password_reset_tokens").delete().eq("token", reset_token);

  res.json({ success: true });
}));

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

// ================= ESP BOARDS + BUILDING LAYOUT =================
// Admin describes the building once (floors -> flats), then maps each board's 8 sensor terminals (TB1..TB8)
// to those flats. A physical board only knows its apartment name + board number (typed once in its WiFi portal);
// it fetches its flat mapping from here every cycle, so no re-flashing is ever needed.

// Boards/layouts are stored under a lower-cased, trimmed apartment key so "Trail Apartments" and "trail apartments" are one apartment.
const boardKey = (a) => String(a || "").trim().toLowerCase();
const BOARD_CHANNELS = 8;
const BOARD_ONLINE_MS = 90 * 1000;
const MAX_FLATS_PER_APARTMENT = 500;

function normalizeChannels(raw) {
  const out = [];
  for (let i = 0; i < BOARD_CHANNELS; i++) {
    const c = Array.isArray(raw) ? raw[i] : null;
    if (c && c.floor && c.flat) out.push({ floor: String(c.floor).trim(), flat: String(c.flat).trim() });
    else out.push(null);
  }
  return out;
}

// Valid apartment names: every admin's apartment, every saved layout, and every apartment that already has owners/invites.
// Priority order decides the canonical spelling (admin's own spelling wins).
async function dbKnownApartments() {
  const [admins, owners, invites, layouts] = await Promise.all([
    dbGetAllAdmins(), dbGetAllFlatOwners(), dbGetUnusedFlatInvites(),
    supabase.from("apartment_layouts").select("apartment").then((r) => r.data || []),
  ]);
  const list = [];
  admins.forEach((a) => a.apartment && list.push(a.apartment));
  layouts.forEach((l) => l.apartment && list.push(l.apartment));
  owners.forEach((u) => u.apartment && list.push(u.apartment));
  invites.forEach((i) => i.apartment && list.push(i.apartment));
  return list;
}
async function resolveApartment(name) {
  const wanted = boardKey(name);
  if (!wanted) return null;
  const known = await dbKnownApartments();
  return known.find((a) => boardKey(a) === wanted) || null;
}

async function dbGetLayout(apartment) {
  const { data } = await supabase.from("apartment_layouts").select("*").eq("apartment_key", boardKey(apartment)).maybeSingle();
  return data;
}
async function dbGetBoard(apartment, boardNo) {
  const { data } = await supabase.from("esp_boards").select("*").eq("apartment", boardKey(apartment)).eq("board_no", boardNo).maybeSingle();
  return data;
}
async function dbGetBoards(apartment) {
  const { data } = await supabase.from("esp_boards").select("*").eq("apartment", boardKey(apartment)).order("board_no", { ascending: true });
  return data || [];
}
function boardWifiResetPending(b) {
  if (!b || !b.wifi_reset_requested_at) return false;
  if (!b.wifi_reset_done_at) return true;
  return new Date(b.wifi_reset_requested_at) > new Date(b.wifi_reset_done_at);
}
function layoutFlatCount(floors) {
  return (Array.isArray(floors) ? floors : []).reduce((n, f) => n + ((f && Array.isArray(f.flats)) ? f.flats.length : 0), 0);
}

// Raise the apartment's "total boards" (never lowers it: an admin can lower it from the Admin page).
async function raiseTotalBoards(apartment, total) {
  const existing = await dbGetLayout(apartment);
  if (existing) {
    if ((existing.total_boards || 0) < total) await supabase.from("apartment_layouts").update({ total_boards: total, updated_at: new Date().toISOString() }).eq("apartment_key", boardKey(apartment));
  } else {
    const admins = await dbGetAllAdmins();
    const adm = admins.find((a) => boardKey(a.apartment) === boardKey(apartment));
    await supabase.from("apartment_layouts").insert({ apartment_key: boardKey(apartment), apartment, admin_phone: adm ? adm.phone : null, floors: [], total_boards: total });
  }
}

// Board asks "what are my channels?" every cycle. Doubles as heartbeat + remote-command channel.
// Optional ?total=N : the "Total boards" value typed in the board's WiFi portal.
app.get("/api/device/config/:apartment/:board", ah(async (req, res) => {
  const boardNo = parseInt(req.params.board, 10);
  if (!boardNo || boardNo < 1 || boardNo > 999) return res.status(400).json({ error: "Invalid board number" });

  const apartment = await resolveApartment(req.params.apartment);
  if (!apartment) return res.status(404).json({ error: "unknown_apartment" });

  const total = parseInt(req.query.total, 10);
  if (total >= 1 && total <= 99) await raiseTotalBoards(apartment, Math.max(total, boardNo));

  let board = await dbGetBoard(apartment, boardNo);
  const now = new Date().toISOString();
  if (!board) {
    await supabase.from("esp_boards").insert({ apartment: boardKey(apartment), board_no: boardNo, channels: [], last_seen: now });
    board = { channels: [] };
  } else {
    await supabase.from("esp_boards").update({ last_seen: now }).eq("apartment", boardKey(apartment)).eq("board_no", boardNo);
  }

  res.json({
    apartment,                                  // canonical spelling — board must use this in its data URLs
    channels: normalizeChannels(board.channels),
    wifi_reset: boardWifiResetPending(board),
  });
}));

app.post("/api/device/wifi-reset-ack/:apartment/:board", ah(async (req, res) => {
  const boardNo = parseInt(req.params.board, 10);
  const apartment = await resolveApartment(req.params.apartment);
  if (!apartment || !boardNo) return res.status(404).json({ error: "unknown_board" });
  await supabase.from("esp_boards").update({ wifi_reset_done_at: new Date().toISOString() }).eq("apartment", boardKey(apartment)).eq("board_no", boardNo);
  res.json({ success: true });
}));

// ---- Admin-side: which apartment may this user manage? ----
async function adminCanUseApartment(user, apartment) {
  if (user.role === "super_admin") return true;
  const lc = boardKey(apartment);
  if (user.apartment && boardKey(user.apartment) === lc) return true;
  const [owners, invites] = await Promise.all([dbGetAllFlatOwners(), dbGetUnusedFlatInvites()]);
  return owners.some((u) => boardKey(u.apartment) === lc && u.managed_by === user.phone)
      || invites.some((i) => boardKey(i.apartment) === lc && i.created_by === user.phone);
}
// Admin: their own apartment always. Super admin: the one they picked.
function requestedApartment(req) {
  if (req.user.role === "admin") return String(req.user.apartment || (req.query && req.query.apartment) || (req.body && req.body.apartment) || "");
  return String((req.query && req.query.apartment) || (req.body && req.body.apartment) || "");
}
async function canonicalApartment(name) { return (await resolveApartment(name)) || String(name || "").trim(); }
// The admin account (phone) that manages an apartment; used as "managed_by" for invites.
async function adminPhoneForApartment(req, apartment) {
  if (req.user.role === "admin") return req.user.phone;
  const admins = await dbGetAllAdmins();
  const a = admins.find((x) => boardKey(x.apartment) === boardKey(apartment));
  return a ? a.phone : null;
}

// Apartment list for the selector (admin: just their own)
app.get("/api/layout/apartments", requireAuth, requireAdmin, ah(async (req, res) => {
  if (req.user.role === "admin") return res.json(req.user.apartment ? [req.user.apartment] : []);
  const admins = await dbGetAllAdmins();
  const seen = new Map();
  admins.forEach((a) => { if (a.apartment && !seen.has(boardKey(a.apartment))) seen.set(boardKey(a.apartment), a.apartment); });
  res.json([...seen.values()].sort());
}));

app.get("/api/layout", requireAuth, requireAdmin, ah(async (req, res) => {
  const apt = requestedApartment(req);
  if (!apt) return res.status(400).json({ error: "apartment is required" });
  if (!(await adminCanUseApartment(req.user, apt))) return res.status(403).json({ error: "Not your apartment" });
  const apartment = await canonicalApartment(apt);
  const l = await dbGetLayout(apartment);
  const floors = (l && l.floors) || [];
  const flats = layoutFlatCount(floors);
  res.json({ apartment, floors, flats, total_boards: (l && l.total_boards) || 0, suggested_boards: Math.ceil(flats / BOARD_CHANNELS) });
}));

// Save floors (floors -> flat numbers) and/or total number of boards
app.post("/api/layout", requireAuth, requireAdmin, ah(async (req, res) => {
  const apt = requestedApartment(req);
  if (!apt) return res.status(400).json({ error: "apartment is required" });
  if (!(await adminCanUseApartment(req.user, apt))) return res.status(403).json({ error: "Not your apartment" });
  const apartment = await canonicalApartment(apt);
  const existing = await dbGetLayout(apartment);

  const update = { updated_at: new Date().toISOString() };

  if (req.body.floors !== undefined) {
    if (!Array.isArray(req.body.floors)) return res.status(400).json({ error: "floors must be a list" });
    const floors = [], floorKeys = new Set();
    let total = 0;
    for (const f of req.body.floors) {
      const key = String((f && f.floor) || "").trim();
      const label = String((f && f.label) || key).trim();
      if (!key) return res.status(400).json({ error: "Every floor needs a name." });
      if (key.includes("/")) return res.status(400).json({ error: `Floor name "${key}" cannot contain "/".` });
      if (floorKeys.has(key.toLowerCase())) return res.status(400).json({ error: `Floor "${label}" is listed twice.` });
      floorKeys.add(key.toLowerCase());
      const flats = [], flatSet = new Set();
      for (const raw of (Array.isArray(f.flats) ? f.flats : [])) {
        const fl = String(raw).trim();
        if (!fl) continue;
        if (fl.includes("/")) return res.status(400).json({ error: `Flat "${fl}" cannot contain "/".` });
        if (flatSet.has(fl.toLowerCase())) return res.status(400).json({ error: `Flat ${fl} appears twice on ${label}.` });
        flatSet.add(fl.toLowerCase());
        flats.push(fl);
      }
      total += flats.length;
      floors.push({ floor: key, label, flats });
    }
    if (total > MAX_FLATS_PER_APARTMENT) return res.status(400).json({ error: `Too many flats (max ${MAX_FLATS_PER_APARTMENT}).` });
    update.floors = floors;
    // keep "total boards" at least big enough for all flats
    const need = Math.ceil(total / BOARD_CHANNELS);
    if ((existing ? existing.total_boards || 0 : 0) < need) update.total_boards = need;
  }
  if (req.body.total_boards !== undefined) {
    const n = parseInt(req.body.total_boards, 10);
    if (isNaN(n) || n < 0 || n > 99) return res.status(400).json({ error: "Total boards must be a number from 0 to 99." });
    update.total_boards = n;
  }

  if (existing) {
    await supabase.from("apartment_layouts").update(update).eq("apartment_key", boardKey(apartment));
  } else {
    await supabase.from("apartment_layouts").insert({
      apartment_key: boardKey(apartment), apartment, admin_phone: await adminPhoneForApartment(req, apartment),
      floors: update.floors || [], total_boards: update.total_boards || 0,
    });
  }
  res.json({ success: true });
}));

// ---- Flat owner invite links for every flat in the layout ----
async function buildLinks(apartment, create, createdBy, only) {
  const l = await dbGetLayout(apartment);
  const floors = (l && l.floors) || [];
  const [owners, invites] = await Promise.all([dbGetAllFlatOwners(), dbGetUnusedFlatInvites()]);
  const out = [];
  for (const fl of floors) {
    for (const flat of fl.flats) {
      const owner = owners.find((u) => boardKey(u.apartment) === boardKey(apartment) && u.floor === fl.floor && u.flat === flat);
      let invite = invites.find((i) => boardKey(i.apartment) === boardKey(apartment) && i.floor === fl.floor && i.flat === flat);
      if (!owner && !invite && create && (!only || (only.floor === fl.floor && only.flat === flat))) {
        const token = crypto.randomBytes(12).toString("hex");
        await dbCreateFlatInvite(token, apartment, fl.floor, flat, createdBy);
        invite = { token };
      }
      out.push({ floor: fl.floor, floor_label: fl.label, flat, owner_name: owner ? owner.name : null, token: (!owner && invite) ? invite.token : null });
    }
  }
  return out;
}
app.get("/api/layout/links", requireAuth, requireAdmin, ah(async (req, res) => {
  const apt = requestedApartment(req);
  if (!apt) return res.status(400).json({ error: "apartment is required" });
  if (!(await adminCanUseApartment(req.user, apt))) return res.status(403).json({ error: "Not your apartment" });
  res.json(await buildLinks(await canonicalApartment(apt), false, null));
}));
app.post("/api/layout/links", requireAuth, requireAdmin, ah(async (req, res) => {
  const apt = requestedApartment(req);
  if (!apt) return res.status(400).json({ error: "apartment is required" });
  if (!(await adminCanUseApartment(req.user, apt))) return res.status(403).json({ error: "Not your apartment" });
  const apartment = await canonicalApartment(apt);
  const createdBy = await adminPhoneForApartment(req, apartment);
  if (!createdBy) return res.status(400).json({ error: "No admin account found for this apartment." });
  const only = (req.body && req.body.floor && req.body.flat) ? { floor: String(req.body.floor), flat: String(req.body.flat) } : null;
  // Regenerate: drop this flat's unused invite(s) so a fresh link is created (the old link stops working)
  if (only && req.body.regenerate) {
    const unused = await dbGetUnusedFlatInvites();
    const tokens = unused.filter((i) => boardKey(i.apartment) === boardKey(apartment) && i.floor === only.floor && i.flat === only.flat).map((i) => i.token);
    if (tokens.length) await supabase.from("flat_invites").delete().in("token", tokens);
  }
  res.json(await buildLinks(apartment, true, createdBy, only));
}));

// ---- Boards ----
app.get("/api/boards", requireAuth, requireAdmin, ah(async (req, res) => {
  const apt = requestedApartment(req);
  if (!apt) return res.status(400).json({ error: "apartment is required" });
  if (!(await adminCanUseApartment(req.user, apt))) return res.status(403).json({ error: "Not your apartment" });
  const apartment = await canonicalApartment(apt);

  const [rows, layout] = await Promise.all([dbGetBoards(apartment), dbGetLayout(apartment)]);
  const total = (layout && layout.total_boards) || 0;
  const maxNo = rows.reduce((m, b) => Math.max(m, b.board_no), total);
  const out = [];
  for (let n = 1; n <= maxNo; n++) {
    const b = rows.find((r) => r.board_no === n);
    if (!b && n > total) continue;
    out.push({
      board_no: n,
      channels: normalizeChannels(b ? b.channels : []),
      last_seen: b ? b.last_seen : null,
      online: !!(b && b.last_seen) && (Date.now() - new Date(b.last_seen).getTime()) <= BOARD_ONLINE_MS,
      wifi_reset_pending: boardWifiResetPending(b),
    });
  }
  res.json({ total_boards: total, boards: out });
}));

app.post("/api/boards/:board/channels", requireAuth, requireAdmin, ah(async (req, res) => {
  const boardNo = parseInt(req.params.board, 10);
  const apt = requestedApartment(req);
  if (!boardNo || boardNo < 1 || boardNo > 999) return res.status(400).json({ error: "Invalid board number" });
  if (!apt) return res.status(400).json({ error: "apartment is required" });
  if (!(await adminCanUseApartment(req.user, apt))) return res.status(403).json({ error: "Not your apartment" });
  const apartment = await canonicalApartment(apt);

  const channels = normalizeChannels(req.body.channels);

  // A flat can be wired to only one channel in the whole apartment
  const seen = new Map();
  for (let i = 0; i < channels.length; i++) {
    const c = channels[i]; if (!c) continue;
    const k = `${c.floor}/${c.flat}`;
    if (seen.has(k)) return res.status(400).json({ error: `Flat ${k} is selected twice on this board (TB${seen.get(k) + 1} and TB${i + 1}).` });
    seen.set(k, i);
  }
  const others = (await dbGetBoards(apartment)).filter((b) => b.board_no !== boardNo);
  for (const b of others) {
    const ch = normalizeChannels(b.channels);
    for (let i = 0; i < ch.length; i++) {
      if (ch[i] && seen.has(`${ch[i].floor}/${ch[i].flat}`)) {
        return res.status(409).json({ error: `Flat ${ch[i].floor}/${ch[i].flat} is already assigned to Board ${b.board_no} (TB${i + 1}). Remove it there first.` });
      }
    }
  }

  const existing = await dbGetBoard(apartment, boardNo);
  if (existing) await supabase.from("esp_boards").update({ channels }).eq("apartment", boardKey(apartment)).eq("board_no", boardNo);
  else await supabase.from("esp_boards").insert({ apartment: boardKey(apartment), board_no: boardNo, channels });
  res.json({ success: true });
}));

app.delete("/api/boards/:board", requireAuth, requireAdmin, ah(async (req, res) => {
  const boardNo = parseInt(req.params.board, 10);
  const apt = requestedApartment(req);
  if (!boardNo || !apt) return res.status(400).json({ error: "board and apartment are required" });
  if (!(await adminCanUseApartment(req.user, apt))) return res.status(403).json({ error: "Not your apartment" });
  await supabase.from("esp_boards").delete().eq("apartment", boardKey(apt)).eq("board_no", boardNo);
  res.json({ success: true });
}));

// Ask every board of an apartment to forget its WiFi (delivered on the board's next 30s poll)
app.post("/api/boards/wifi-reset", requireAuth, requireAdmin, ah(async (req, res) => {
  const apt = requestedApartment(req);
  if (!apt) return res.status(400).json({ error: "apartment is required" });
  if (!(await adminCanUseApartment(req.user, apt))) return res.status(403).json({ error: "Not your apartment" });
  const boards = await dbGetBoards(apt);
  if (boards.length === 0) return res.status(404).json({ error: "No boards have connected for this apartment yet." });
  await supabase.from("esp_boards").update({ wifi_reset_requested_at: new Date().toISOString() }).eq("apartment", boardKey(apt));
  res.json({ success: true, boards: boards.length });
}));

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
    const { sensor_status, last_seen } = sensorStatusFor(flatData ? flatData.received_at : null);

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
      sensor_status,
      last_seen,
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
  const latest = active ? await dbGetFlatData(key) : null;
  const { sensor_status, last_seen } = sensorStatusFor(latest ? latest.received_at : null);

  res.json({
    subscription_active: active,
    has_valve: hasValve,
    latest,
    history: active ? await dbGetHistory(key) : [],
    limit,
    subscription: {
      is_trial: isTrial,
      valid_until: reference.toISOString(),
      days_remaining: daysRemaining,
    },
    sensor_status,
    last_seen,
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
    subscription_amount_paise: await getSubscriptionAmount(),
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

app.post("/api/settings/subscription-amount", requireAuth, requireSuperAdmin, ah(async (req, res) => {
  const rupees = parseFloat(req.body.subscription_amount_rupees);
  if (!rupees || rupees <= 0) return res.status(400).json({ error: "Enter a valid amount in rupees" });
  const paise = Math.round(rupees * 100);
  await dbSetSetting("subscription_amount_paise", paise);
  res.json({ success: true, subscription_amount_paise: paise });
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

const RAZORPAY_WEBHOOK_SECRET = process.env.RAZORPAY_WEBHOOK_SECRET;

// Server-to-server safety net: in case the browser closes/loses network right after payment
// (before our client-side /api/payment/verify call completes), this webhook independently
// confirms the payment and extends the subscription. It's idempotent — if the client-side
// verify already handled this order, the pending order row is gone and this becomes a no-op.
app.post("/api/razorpay/webhook", ah(async (req, res) => {
  const signature = req.headers["x-razorpay-signature"];
  if (!RAZORPAY_WEBHOOK_SECRET || !signature || !req.rawBody) return res.status(400).json({ error: "Webhook not configured" });

  const expected = crypto.createHmac("sha256", RAZORPAY_WEBHOOK_SECRET).update(req.rawBody).digest("hex");
  if (expected !== signature) return res.status(400).json({ error: "Invalid webhook signature" });

  const event = req.body.event;
  if (event !== "payment.captured" && event !== "order.paid") return res.json({ received: true, ignored: event });

  const orderId = req.body.payload?.payment?.entity?.order_id || req.body.payload?.order?.entity?.id;
  if (!orderId) return res.json({ received: true, note: "No order_id in payload" });

  try {
    const selfOrder = await dbGetPaymentOrder(orderId);
    if (selfOrder) {
      const newPaidUntil = new Date(new Date(selfOrder.base_due).getTime() + selfOrder.periods * CYCLE_MS).toISOString();
      await dbSetSubscription(selfOrder.key, newPaidUntil);
      await dbDeletePaymentOrder(orderId);
      console.log(`[webhook] Subscription extended for ${selfOrder.key} by ${selfOrder.periods} cycle(s), now valid until ${newPaidUntil}`);
      return res.json({ received: true, applied: true });
    }

    const adminEntries = await dbGetAdminOrder(orderId);
    if (adminEntries) {
      for (const entry of adminEntries) {
        const newPaidUntil = new Date(new Date(entry.base_due).getTime() + entry.cycles * CYCLE_MS).toISOString();
        await dbSetSubscription(entry.key, newPaidUntil);
        console.log(`[webhook] Subscription extended (admin) for ${entry.key} by ${entry.cycles} cycle(s), now valid until ${newPaidUntil}`);
      }
      await dbDeleteAdminOrder(orderId);
      return res.json({ received: true, applied: true });
    }

    // Order not found — most likely already applied by the client-side verify call. Nothing to do.
    res.json({ received: true, applied: false, note: "Order already processed or not found" });
  } catch (e) {
    console.error("[webhook] Error applying payment:", e);
    res.json({ received: true, applied: false, error: "Internal error, will not retry" });
  }
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
