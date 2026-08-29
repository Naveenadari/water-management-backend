/* ==========================================================
   WATER MANAGEMENT SYSTEM - BACKEND SERVER (HTTP-ONLY + AUTH)
   ========================================================== */

const express = require("express");
const cors = require("cors");
const crypto = require("crypto");

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static("public"));

const PORT = process.env.PORT || 3000;

// ---------------- IN-MEMORY DATA STORE ----------------
// NOTE: This resets whenever the server restarts (e.g. free-tier sleep).
// For production, replace with a real database (e.g. Supabase/Postgres).
const flatsData = {};
const history = {};
const pendingCommands = {};
const users = {};      // key: phone -> user record
const sessions = {};   // key: token -> phone
const limits = {};     // key: apartment/floor/flat -> daily liter limit

const MAX_HISTORY = 200;

function keyFor(apartment, floor, flat) {
  return `${apartment}/${floor}/${flat}`;
}

function hashPassword(password, salt) {
  return crypto.scryptSync(password, salt, 64).toString("hex");
}

function makeToken() {
  return crypto.randomBytes(24).toString("hex");
}

// ---------------- AUTH MIDDLEWARE ----------------
function requireAuth(req, res, next) {
  const authHeader = req.headers.authorization || "";
  const token = authHeader.replace("Bearer ", "");
  const phone = sessions[token];
  if (!phone || !users[phone]) {
    return res.status(401).json({ error: "Not authenticated" });
  }
  req.user = users[phone];
  next();
}

function requireAdmin(req, res, next) {
  if (req.user.role !== "admin") {
    return res.status(403).json({ error: "Admin only" });
  }
  next();
}

// ================= AUTH ROUTES =================

// Signup: role = "admin" or "flat_owner"
app.post("/api/auth/signup", (req, res) => {
  const { role, name, phone, password, apartment, floor, flat } = req.body;

  if (!role || !name || !phone || !password) {
    return res.status(400).json({ error: "Missing required fields" });
  }
  if (role === "flat_owner" && (!apartment || !floor || !flat)) {
    return res.status(400).json({ error: "Flat owner must provide apartment/floor/flat" });
  }
  if (users[phone]) {
    return res.status(409).json({ error: "An account with this phone number already exists" });
  }

  const salt = crypto.randomBytes(16).toString("hex");
  const passwordHash = hashPassword(password, salt);

  users[phone] = {
    role, name, phone, salt, passwordHash,
    apartment: apartment || null, floor: floor || null, flat: flat || null,
    createdAt: new Date().toISOString(),
  };

  const token = makeToken();
  sessions[token] = phone;

  const { salt: _s, passwordHash: _p, ...safeUser } = users[phone];
  res.json({ success: true, token, user: safeUser });
});

// Login
app.post("/api/auth/login", (req, res) => {
  const { phone, password } = req.body;
  const user = users[phone];
  if (!user) return res.status(401).json({ error: "Invalid phone number or password" });

  const attemptHash = hashPassword(password, user.salt);
  if (attemptHash !== user.passwordHash) {
    return res.status(401).json({ error: "Invalid phone number or password" });
  }

  const token = makeToken();
  sessions[token] = phone;

  const { salt: _s, passwordHash: _p, ...safeUser } = user;
  res.json({ success: true, token, user: safeUser });
});

// Get current logged-in user
app.get("/api/auth/me", requireAuth, (req, res) => {
  const { salt: _s, passwordHash: _p, ...safeUser } = req.user;
  res.json({ user: safeUser });
});

// ---------------- HEALTH CHECK ----------------
app.get("/api/health", (req, res) => {
  res.json({ status: "ok", time: new Date().toISOString() });
});

// ================= ESP32 -> BACKEND (no auth, device-to-server) =================

app.post("/api/device/data/:apartment/:floor/:flat", (req, res) => {
  const { apartment, floor, flat } = req.params;
  const key = keyFor(apartment, floor, flat);
  const { flow_lpm, total_liters, valve_status } = req.body;

  const record = {
    apartment, floor, flat,
    flow_lpm, total_liters, valve_status,
    received_at: new Date().toISOString(),
  };

  flatsData[key] = record;
  if (!history[key]) history[key] = [];
  history[key].push(record);
  if (history[key].length > MAX_HISTORY) history[key].shift();

  console.log(`Data from ${key}:`, record);
  res.json({ success: true });
});

app.get("/api/device/command/:apartment/:floor/:flat", (req, res) => {
  const { apartment, floor, flat } = req.params;
  const key = keyFor(apartment, floor, flat);
  const command = pendingCommands[key] || null;
  pendingCommands[key] = null;
  res.json({ command });
});

// ================= APP/DASHBOARD -> BACKEND (auth required) =================

// Admin: get all flats' latest data
app.get("/api/flats", requireAuth, requireAdmin, (req, res) => {
  res.json(Object.values(flatsData));
});

// Get one flat's data — flat owner can only view their own flat; admin can view any
app.get("/api/flats/:apartment/:floor/:flat", requireAuth, (req, res) => {
  const { apartment, floor, flat } = req.params;
  const key = keyFor(apartment, floor, flat);

  if (req.user.role !== "admin") {
    const ownKey = keyFor(req.user.apartment, req.user.floor, req.user.flat);
    if (ownKey !== key) {
      return res.status(403).json({ error: "You can only view your own flat" });
    }
  }

  res.json({
    latest: flatsData[key] || null,
    history: history[key] || [],
    limit: limits[key] || null,
  });
});

// Set valve command — flat owner can control only their own flat; admin can control any
app.post("/api/valve/:apartment/:floor/:flat", requireAuth, (req, res) => {
  const { apartment, floor, flat } = req.params;
  const { action } = req.body;
  const key = keyFor(apartment, floor, flat);

  if (req.user.role !== "admin") {
    const ownKey = keyFor(req.user.apartment, req.user.floor, req.user.flat);
    if (ownKey !== key) {
      return res.status(403).json({ error: "You can only control your own flat's valve" });
    }
  }

  if (!["OPEN", "CLOSE"].includes(action)) {
    return res.status(400).json({ error: "action must be OPEN or CLOSE" });
  }

  pendingCommands[key] = action;
  console.log(`Command queued for ${key}: ${action} (by ${req.user.role} ${req.user.name})`);
  res.json({ success: true, apartment, floor, flat, action });
});

// Admin: set a daily liter limit for a flat
app.post("/api/limits/:apartment/:floor/:flat", requireAuth, requireAdmin, (req, res) => {
  const { apartment, floor, flat } = req.params;
  const { daily_limit_liters } = req.body;
  const key = keyFor(apartment, floor, flat);
  limits[key] = daily_limit_liters;
  res.json({ success: true, key, daily_limit_liters });
});

// ---------------- START SERVER ----------------
app.listen(PORT, () => {
  console.log(`Backend (HTTP + Auth) running on port ${PORT}`);
});
