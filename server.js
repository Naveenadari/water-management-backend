/* ==========================================================
   WATER MANAGEMENT SYSTEM - BACKEND SERVER
   - Subscribes to MQTT broker (HiveMQ Cloud)
   - Stores latest flow/valve data per flat in memory
   - Exposes REST API for dashboard (Admin + Flat Owner)
   - Publishes valve OPEN/CLOSE commands back to ESP32 devices
   ========================================================== */

require("dotenv").config();
const express = require("express");
const cors = require("cors");
const mqtt = require("mqtt");

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static("public")); // serves the test page at public/index.html

const PORT = process.env.PORT || 3000;

// ---------------- MQTT CONFIG (from environment variables) ----------------
const MQTT_HOST = process.env.MQTT_HOST; // e.g. c22682f2c9de462ebdfc2dd74d48ac71.s1.eu.hivemq.cloud
const MQTT_PORT = process.env.MQTT_PORT || 8883;
const MQTT_USER = process.env.MQTT_USER;
const MQTT_PASS = process.env.MQTT_PASS;

const MQTT_URL = `mqtts://${MQTT_HOST}:${MQTT_PORT}`;

// ---------------- IN-MEMORY DATA STORE ----------------
// In production this would be a real database (Postgres/TimescaleDB).
// For now, we keep latest reading + a short history per flat in memory.
const flatsData = {}; // key: "apartment/floor/flat" -> { flow_lpm, total_liters, valve_status, timestamp }
const history = {};   // key: "apartment/floor/flat" -> array of readings (capped)

const MAX_HISTORY = 200;

function keyFor(apartment, floor, flat) {
  return `${apartment}/${floor}/${flat}`;
}

// ---------------- MQTT CLIENT ----------------
console.log("Connecting to MQTT broker:", MQTT_URL);

const mqttClient = mqtt.connect(MQTT_URL, {
  username: MQTT_USER,
  password: MQTT_PASS,
  reconnectPeriod: 3000,
  clientId: "backend-server-" + Math.random().toString(16).slice(2, 8),
});

mqttClient.on("connect", () => {
  console.log("MQTT connected!");
  // Subscribe to all data + valve status topics from all apartments/floors/flats
  mqttClient.subscribe("+/+/+/data", (err) => {
    if (!err) console.log("Subscribed to +/+/+/data");
  });
  mqttClient.subscribe("+/+/+/valve/status", (err) => {
    if (!err) console.log("Subscribed to +/+/+/valve/status");
  });
});

mqttClient.on("error", (err) => {
  console.error("MQTT error:", err.message);
});

mqttClient.on("reconnect", () => {
  console.log("MQTT reconnecting...");
});

// ---------------- HANDLE INCOMING MESSAGES ----------------
mqttClient.on("message", (topic, payload) => {
  const parts = topic.split("/");
  // topic format: apartment/floor/flat/data  OR  apartment/floor/flat/valve/status
  const [apartment, floor, flat] = parts;
  const key = keyFor(apartment, floor, flat);

  if (topic.endsWith("/data")) {
    try {
      const data = JSON.parse(payload.toString());
      const record = {
        apartment,
        floor,
        flat,
        flow_lpm: data.flow_lpm,
        total_liters: data.total_liters,
        valve_status: data.valve_status,
        device_timestamp: data.timestamp,
        received_at: new Date().toISOString(),
      };

      flatsData[key] = record;

      if (!history[key]) history[key] = [];
      history[key].push(record);
      if (history[key].length > MAX_HISTORY) history[key].shift();

      console.log(`Data [${key}]:`, record);
    } catch (e) {
      console.error("Bad JSON payload on", topic, payload.toString());
    }
  }

  if (topic.endsWith("/valve/status")) {
    const status = payload.toString();
    if (!flatsData[key]) flatsData[key] = { apartment, floor, flat };
    flatsData[key].valve_status = status;
    flatsData[key].valve_updated_at = new Date().toISOString();
    console.log(`Valve status [${key}]: ${status}`);
  }
});

// ---------------- REST API ----------------

// Health check
app.get("/api/health", (req, res) => {
  res.json({
    status: "ok",
    mqtt_connected: mqttClient.connected,
    time: new Date().toISOString(),
  });
});

// Get all flats' latest data (Admin dashboard view)
app.get("/api/flats", (req, res) => {
  res.json(Object.values(flatsData));
});

// Get single flat's latest data + history (Flat Owner dashboard view)
app.get("/api/flats/:apartment/:floor/:flat", (req, res) => {
  const { apartment, floor, flat } = req.params;
  const key = keyFor(apartment, floor, flat);
  res.json({
    latest: flatsData[key] || null,
    history: history[key] || [],
  });
});

// Send valve OPEN/CLOSE command (used by both Admin and Flat Owner apps)
app.post("/api/valve/:apartment/:floor/:flat", (req, res) => {
  const { apartment, floor, flat } = req.params;
  const { action } = req.body; // "OPEN" or "CLOSE"

  if (!["OPEN", "CLOSE"].includes(action)) {
    return res.status(400).json({ error: "action must be OPEN or CLOSE" });
  }

  const topic = `${apartment}/${floor}/${flat}/valve/set`;
  mqttClient.publish(topic, action, {}, (err) => {
    if (err) {
      console.error("Publish failed:", err);
      return res.status(500).json({ error: "Failed to publish command" });
    }
    console.log(`Published to ${topic}: ${action}`);
    res.json({ success: true, topic, action });
  });
});

// ---------------- START SERVER ----------------
app.listen(PORT, () => {
  console.log(`Backend server running on port ${PORT}`);
});
