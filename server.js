/* ==========================================================
   WATER MANAGEMENT SYSTEM - BACKEND SERVER (HTTP-ONLY VERSION)
   No MQTT broker needed. ESP32 talks to this server directly:
   - ESP32 POSTs flow data periodically
   - ESP32 GETs pending valve commands periodically (polling)
   - Dashboard/App reads flat data + sets valve commands via REST
   ========================================================== */

const express = require("express");
const cors = require("cors");

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static("public"));

const PORT = process.env.PORT || 3000;

// ---------------- IN-MEMORY DATA STORE ----------------
const flatsData = {};      // key -> latest reading
const history = {};        // key -> array of readings
const pendingCommands = {}; // key -> "OPEN" | "CLOSE" | null (cleared once ESP32 fetches it)

const MAX_HISTORY = 200;

function keyFor(apartment, floor, flat) {
  return `${apartment}/${floor}/${flat}`;
}

// ---------------- HEALTH CHECK ----------------
app.get("/api/health", (req, res) => {
  res.json({ status: "ok", time: new Date().toISOString() });
});

// ================= ESP32 -> BACKEND =================

// ESP32 posts flow/valve data here every ~30-60s
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

// ESP32 polls this every ~5-10s to check for a pending valve command
app.get("/api/device/command/:apartment/:floor/:flat", (req, res) => {
  const { apartment, floor, flat } = req.params;
  const key = keyFor(apartment, floor, flat);

  const command = pendingCommands[key] || null;
  pendingCommands[key] = null; // clear after delivering once

  res.json({ command });
});

// ================= APP/DASHBOARD -> BACKEND =================

// Get all flats' latest data (Admin view)
app.get("/api/flats", (req, res) => {
  res.json(Object.values(flatsData));
});

// Get one flat's latest data + history (Flat Owner view)
app.get("/api/flats/:apartment/:floor/:flat", (req, res) => {
  const { apartment, floor, flat } = req.params;
  const key = keyFor(apartment, floor, flat);
  res.json({
    latest: flatsData[key] || null,
    history: history[key] || [],
  });
});

// App sets a valve command; ESP32 will pick it up on its next poll
app.post("/api/valve/:apartment/:floor/:flat", (req, res) => {
  const { apartment, floor, flat } = req.params;
  const { action } = req.body;

  if (!["OPEN", "CLOSE"].includes(action)) {
    return res.status(400).json({ error: "action must be OPEN or CLOSE" });
  }

  const key = keyFor(apartment, floor, flat);
  pendingCommands[key] = action;
  console.log(`Command queued for ${key}: ${action}`);

  res.json({ success: true, apartment, floor, flat, action });
});

// ---------------- START SERVER ----------------
app.listen(PORT, () => {
  console.log(`HTTP-only backend running on port ${PORT}`);
});
