# Water Management Backend

ESP32 → MQTT (HiveMQ Cloud) → this backend → Dashboard/API

## What this does
- Connects to your HiveMQ Cloud broker
- Listens to all flats' flow data and valve status
- Provides simple REST API endpoints
- Includes a built-in test page (no 3rd-party app needed) at `/`

## API Endpoints
- `GET /api/health` — check server + MQTT connection status
- `GET /api/flats` — all flats' latest data (admin view)
- `GET /api/flats/:apartment/:floor/:flat` — one flat's latest data + history
- `POST /api/valve/:apartment/:floor/:flat` — body: `{ "action": "OPEN" }` or `"CLOSE"`

## Deploy Steps (GitHub + Render — both free)

### 1. Push this code to GitHub
1. Create a new repository on github.com (e.g. `water-management-backend`)
2. Upload all these files to it (via GitHub web upload, or git commands)

### 2. Deploy on Render
1. Go to render.com → Sign up/Login
2. Click "New +" → "Web Service"
3. Connect your GitHub repo
4. Settings:
   - Build Command: `npm install`
   - Start Command: `npm start`
5. Add Environment Variables (Render dashboard → Environment tab):
   - `MQTT_HOST` = c22682f2c9de462ebdfc2dd74d48ac71.s1.eu.hivemq.cloud
   - `MQTT_PORT` = 8883
   - `MQTT_USER` = your HiveMQ username
   - `MQTT_PASS` = your HiveMQ password
6. Click "Create Web Service" — Render will build and deploy automatically

### 3. Test it
Once deployed, Render gives you a URL like:
```
https://water-management-backend.onrender.com
```
Open that URL in your browser — you'll see the built-in test page.
Use it to Open/Close the valve and view flat data, without any external app.

## Note
Render's free tier sleeps after inactivity — first request after idle may take
10-20 seconds to wake up. This is fine for testing; for production a paid tier
avoids this delay.
