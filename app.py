import asyncio
import json
import os
import subprocess
import time
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
import paho.mqtt.client as mqtt
import websockets

# Configuration
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "#"  # Subscribe to all topics
HTTP_PORT = int(os.environ.get("PORT", 8080))
WS_PORT = 8765

# Store connected WebSocket clients
ws_clients = set()
message_history = []  # Keep last 50 messages

def start_mosquitto():
    """Start Mosquitto broker as subprocess"""
    conf_path = os.path.join(os.path.dirname(__file__), "mosquitto.conf")
    print(f"[MOSQUITTO] Starting with config: {conf_path}")
    proc = subprocess.Popen(
        ["mosquitto", "-c", conf_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )
    time.sleep(2)  # Wait for broker to start
    print("[MOSQUITTO] Started")
    return proc

# MQTT client to receive messages and forward to WebSocket
mqtt_client = mqtt.Client(client_id="dashboard_subscriber")

async def broadcast_message(data):
    """Send message to all connected WebSocket clients"""
    if ws_clients:
        msg = json.dumps(data)
        await asyncio.gather(
            *[client.send(msg) for client in list(ws_clients)],
            return_exceptions=True
        )

def on_mqtt_message(client, userdata, msg):
    """Called when MQTT message received - forward to WebSocket clients"""
    data = {
        "topic": msg.topic,
        "payload": msg.payload.decode("utf-8", errors="replace"),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "qos": msg.qos
    }
    message_history.append(data)
    if len(message_history) > 50:
        message_history.pop(0)
    print(f"[MQTT] {data['timestamp']} | {msg.topic}: {data['payload']}")
    # Schedule async broadcast
    try:
        loop = asyncio.get_event_loop()
        asyncio.run_coroutine_threadsafe(broadcast_message(data), loop)
    except Exception as e:
        print(f"[WS BROADCAST ERROR] {e}")

def on_mqtt_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[MQTT CLIENT] Connected to broker")
        client.subscribe(MQTT_TOPIC)
        print(f"[MQTT CLIENT] Subscribed to: {MQTT_TOPIC}")
    else:
        print(f"[MQTT CLIENT] Connection failed with code {rc}")

async def ws_handler(websocket):
    """Handle WebSocket connections from browser"""
    ws_clients.add(websocket)
    print(f"[WS] Client connected. Total: {len(ws_clients)}")
    try:
        # Send message history on connect
        for msg in message_history:
            await websocket.send(json.dumps(msg))
        # Keep alive
        async for message in websocket:
            pass
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        ws_clients.discard(websocket)
        print(f"[WS] Client disconnected. Total: {len(ws_clients)}")

HTML_CONTENT = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MQTT Dashboard - Railway Test</title>
    <script src="https://cdn.jsdelivr.net/npm/alpinejs@3.x.x/dist/cdn.min.js" defer></script>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; padding: 20px; }
        h1 { color: #38bdf8; margin-bottom: 8px; font-size: 1.8rem; }
        .subtitle { color: #94a3b8; margin-bottom: 24px; font-size: 0.9rem; }
        .status-bar { display: flex; gap: 16px; margin-bottom: 20px; flex-wrap: wrap; }
        .status-badge { padding: 6px 14px; border-radius: 20px; font-size: 0.8rem; font-weight: 600; }
        .connected { background: #065f46; color: #6ee7b7; }
        .disconnected { background: #7f1d1d; color: #fca5a5; }
        .stat { background: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 12px 18px; }
        .stat-label { color: #64748b; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 1px; }
        .stat-value { color: #38bdf8; font-size: 1.5rem; font-weight: bold; }
        .messages-panel { background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 16px; }
        .messages-header { color: #94a3b8; font-size: 0.85rem; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 12px; display: flex; justify-content: space-between; }
        .message-item { background: #0f172a; border-left: 3px solid #38bdf8; padding: 10px 14px; margin-bottom: 8px; border-radius: 0 6px 6px 0; animation: fadeIn 0.3s ease; }
        .message-item.new { border-left-color: #22c55e; }
        .message-item.pc { border-left-color: #f59e0b; }
        .msg-topic { color: #38bdf8; font-size: 0.85rem; font-weight: 600; }
        .msg-payload { color: #e2e8f0; margin: 4px 0; word-break: break-all; }
        .msg-meta { color: #475569; font-size: 0.75rem; }
        .msg-source { display: inline-block; padding: 1px 8px; border-radius: 10px; font-size: 0.7rem; margin-left: 6px; }
        .source-railway { background: #1e40af; color: #93c5fd; }
        .source-pc { background: #78350f; color: #fcd34d; }
        @keyframes fadeIn { from { opacity: 0; transform: translateX(-10px); } to { opacity: 1; transform: translateX(0); } }
        .empty-state { text-align: center; color: #475569; padding: 40px; font-size: 0.9rem; }
        .pulse { display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: #22c55e; margin-right: 6px; animation: pulse 1.5s infinite; }
        @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.3; } }
    </style>
</head>
<body>
    <div x-data="mqttDashboard()" x-init="init()">
        <h1>MQTT Dashboard</h1>
        <p class="subtitle">Railway Server - Real-time MQTT Message Monitor (20260706)</p>

        <div class="status-bar">
            <div :class="wsConnected ? 'status-badge connected' : 'status-badge disconnected'">
                <span x-show="wsConnected"><span class="pulse"></span>WebSocket Connected</span>
                <span x-show="!wsConnected">WebSocket Disconnected</span>
            </div>
            <div class="stat">
                <div class="stat-label">Total Messages</div>
                <div class="stat-value" x-text="messages.length"></div>
            </div>
            <div class="stat">
                <div class="stat-label">From Railway</div>
                <div class="stat-value" x-text="messages.filter(m => m.topic && m.topic.includes('railway')).length"></div>
            </div>
            <div class="stat">
                <div class="stat-label">From PC</div>
                <div class="stat-value" x-text="messages.filter(m => m.topic && m.topic.includes('pc')).length"></div>
            </div>
        </div>

        <div class="messages-panel">
            <div class="messages-header">
                <span>Live Messages (newest first)</span>
                <span x-text="messages.length + ' messages'"></span>
            </div>
            <template x-if="messages.length === 0">
                <div class="empty-state">Waiting for MQTT messages...<br>Send from Railway publisher or PC client</div>
            </template>
            <template x-for="(msg, index) in messages" :key="index">
                <div :class="'message-item ' + (index === 0 ? 'new' : '') + (msg.topic && msg.topic.includes('pc') ? ' pc' : '')">
                    <div class="msg-topic">
                        <span x-text="msg.topic"></span>
                        <span :class="msg.topic && msg.topic.includes('pc') ? 'msg-source source-pc' : 'msg-source source-railway'">
                            <span x-text="msg.topic && msg.topic.includes('pc') ? 'PC' : 'Railway'"></span>
                        </span>
                    </div>
                    <div class="msg-payload" x-text="msg.payload"></div>
                    <div class="msg-meta" x-text="msg.timestamp + ' | QoS: ' + msg.qos"></div>
                </div>
            </template>
        </div>
    </div>

    <script>
        function mqttDashboard() {
            return {
                messages: [],
                wsConnected: false,
                ws: null,

                init() {
                    this.connectWS();
                },

                connectWS() {
                    const wsUrl = (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.hostname + ':' + (location.port || (location.protocol === 'https:' ? 443 : 80)) + '/ws';
                    // For Railway, use same host with /ws path via nginx-like proxy
                    // Actually connect to WS_PORT or via proxy
                    const wsHost = location.hostname;
                    const wsPort = 8765;
                    const url = 'ws://' + wsHost + ':' + wsPort;
                    console.log('Connecting to WebSocket:', url);
                    this.ws = new WebSocket(url);
                    this.ws.onopen = () => {
                        this.wsConnected = true;
                        console.log('WS connected');
                    };
                    this.ws.onmessage = (event) => {
                        const data = JSON.parse(event.data);
                        this.messages.unshift(data);
                        if (this.messages.length > 50) this.messages.pop();
                    };
                    this.ws.onclose = () => {
                        this.wsConnected = false;
                        console.log('WS disconnected, reconnecting in 3s...');
                        setTimeout(() => this.connectWS(), 3000);
                    };
                    this.ws.onerror = (err) => {
                        console.error('WS error:', err);
                    };
                }
            };
        }
    </script>
</body>
</html>
"""

class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.end_headers()
        self.wfile.write(HTML_CONTENT.encode('utf-8'))

    def log_message(self, format, *args):
        pass  # Suppress logs

def run_http_server():
    server = HTTPServer(('0.0.0.0', HTTP_PORT), DashboardHandler)
    print(f"[HTTP] Dashboard running on port {HTTP_PORT}")
    server.serve_forever()

async def run_ws_server():
    print(f"[WS] WebSocket server running on port {WS_PORT}")
    async with websockets.serve(ws_handler, "0.0.0.0", WS_PORT):
        await asyncio.Future()  # Run forever

def start_mqtt_client():
    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_message = on_mqtt_message
    time.sleep(2)  # Wait for Mosquitto
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start()
        print("[MQTT CLIENT] Started")
    except Exception as e:
        print(f"[MQTT CLIENT] Failed to connect: {e}")

if __name__ == '__main__':
    print("=== MQTT Dashboard Server Starting ===")

    # Start Mosquitto broker
    mosquitto_proc = start_mosquitto()

    # Start MQTT subscriber client in thread
    mqtt_thread = threading.Thread(target=start_mqtt_client, daemon=True)
    mqtt_thread.start()

    # Start HTTP server in thread
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()

    # Run WebSocket server (async)
    asyncio.run(run_ws_server())
