import asyncio
import json
import os
import subprocess
import time
import threading
from datetime import datetime

import paho.mqtt.client as mqtt
from aiohttp import web
import aiohttp

# ── Configuration ──────────────────────────────────────────────────────────────
MQTT_BROKER   = "localhost"
MQTT_PORT     = 1883
MQTT_TOPIC    = "#"   # subscribe to everything
HTTP_PORT     = int(os.environ.get("PORT", 8080))

# ── Shared state ───────────────────────────────────────────────────────────────
ws_clients      = set()          # connected browser WebSocket sessions
message_history = []             # last 50 MQTT messages
_loop           = None           # main asyncio event loop (set in main)

# ── Mosquitto ──────────────────────────────────────────────────────────────────
def start_mosquitto():
    conf = os.path.join(os.path.dirname(__file__), "mosquitto.conf")
    print(f"[MOSQUITTO] starting with config: {conf}")
    proc = subprocess.Popen(
        ["mosquitto", "-c", conf],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    time.sleep(2)
    print("[MOSQUITTO] broker up")
    return proc

# ── MQTT subscriber (runs in its own thread via paho loop_start) ───────────────
async def _broadcast(data: dict):
    msg = json.dumps(data)
    dead = set()
    for ws in list(ws_clients):
        try:
            await ws.send_str(msg)
        except Exception:
            dead.add(ws)
    ws_clients.difference_update(dead)

def on_mqtt_message(client, userdata, msg):
    data = {
        "topic":     msg.topic,
        "payload":   msg.payload.decode("utf-8", errors="replace"),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "qos":       msg.qos,
    }
    message_history.append(data)
    if len(message_history) > 50:
        message_history.pop(0)
    print(f"[MQTT] {data['timestamp']} | {msg.topic}: {data['payload']}")
    if _loop:
        asyncio.run_coroutine_threadsafe(_broadcast(data), _loop)

def on_mqtt_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[MQTT-CLIENT] connected to broker")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"[MQTT-CLIENT] connect failed rc={rc}")

def start_mqtt_subscriber():
    c = mqtt.Client(client_id="dashboard_sub")
    c.on_connect = on_mqtt_connect
    c.on_message = on_mqtt_message
    time.sleep(3)  # wait for mosquitto
    for attempt in range(10):
        try:
            c.connect(MQTT_BROKER, MQTT_PORT, 60)
            c.loop_start()
            print(f"[MQTT-CLIENT] started (attempt {attempt+1})")
            return
        except Exception as e:
            print(f"[MQTT-CLIENT] attempt {attempt+1} failed: {e}")
            time.sleep(2)
    print("[MQTT-CLIENT] gave up connecting")

# ── Railway-side publisher (Subtask 2, runs as background thread) ──────────────
def start_railway_publisher():
    import importlib.util, sys
    spec = importlib.util.spec_from_file_location(
        "publisher",
        os.path.join(os.path.dirname(__file__), "publisher.py")
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.run_publisher()

# ── HTML Dashboard ─────────────────────────────────────────────────────────────
HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>MQTT Dashboard - 20260706</title>
  <script src="https://cdn.jsdelivr.net/npm/alpinejs@3.x.x/dist/cdn.min.js" defer></script>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{font-family:'Segoe UI',sans-serif;background:#0f172a;color:#e2e8f0;padding:20px}
    h1{color:#38bdf8;font-size:1.8rem;margin-bottom:6px}
    .sub{color:#94a3b8;font-size:.85rem;margin-bottom:20px}
    .bar{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:20px;align-items:center}
    .badge{padding:5px 14px;border-radius:20px;font-size:.8rem;font-weight:600}
    .ok{background:#065f46;color:#6ee7b7}.err{background:#7f1d1d;color:#fca5a5}
    .stat{background:#1e293b;border:1px solid #334155;border-radius:8px;padding:10px 16px}
    .sl{color:#64748b;font-size:.7rem;text-transform:uppercase;letter-spacing:1px}
    .sv{color:#38bdf8;font-size:1.4rem;font-weight:700}
    .panel{background:#1e293b;border:1px solid #334155;border-radius:10px;padding:16px}
    .ph{color:#94a3b8;font-size:.8rem;text-transform:uppercase;letter-spacing:1px;margin-bottom:12px;display:flex;justify-content:space-between}
    .item{background:#0f172a;border-left:3px solid #38bdf8;padding:9px 13px;margin-bottom:7px;border-radius:0 6px 6px 0;animation:fi .3s ease}
    .item.pc{border-left-color:#f59e0b}
    .tp{color:#38bdf8;font-size:.83rem;font-weight:600}
    .tp.pc{color:#f59e0b}
    .pl{margin:4px 0;word-break:break-all}
    .mt{color:#475569;font-size:.73rem}
    .src{display:inline-block;padding:1px 8px;border-radius:10px;font-size:.7rem;margin-left:6px}
    .sr{background:#1e40af;color:#93c5fd}.sp{background:#78350f;color:#fcd34d}
    @keyframes fi{from{opacity:0;transform:translateX(-8px)}to{opacity:1;transform:translateX(0)}}
    .empty{text-align:center;color:#475569;padding:36px;font-size:.9rem}
    .dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:#22c55e;margin-right:5px;animation:pu 1.5s infinite}
    @keyframes pu{0%,100%{opacity:1}50%{opacity:.3}}
    .info-box{background:#1e3a5f;border:1px solid #2563eb;border-radius:8px;padding:12px 16px;margin-bottom:16px;font-size:.82rem;color:#93c5fd;line-height:1.6}
    .info-box b{color:#38bdf8}
  </style>
</head>
<body x-data="app()" x-init="init()">
  <h1>MQTT Dashboard</h1>
  <p class="sub">Railway Server - Real-time Monitor (20260706) | Topic filter: <b>#</b></p>

  <div class="info-box">
    <b>Connection Info for PC Publisher:</b><br>
    MQTT Broker Host: <b id="hostDisplay">loading...</b><br>
    MQTT TCP Port: <b>1883</b> &nbsp;|&nbsp; WebSocket Port: <b>9001</b><br>
    Topics: <b>railway/test</b> (Railway publisher) | <b>pc/test</b> (PC publisher)
  </div>

  <div class="bar">
    <div :class="ws ? 'badge ok' : 'badge err'">
      <span x-show="ws"><span class="dot"></span>WS Connected</span>
      <span x-show="!ws">WS Disconnected</span>
    </div>
    <div class="stat"><div class="sl">Total</div><div class="sv" x-text="msgs.length"></div></div>
    <div class="stat"><div class="sl">From Railway</div><div class="sv" x-text="msgs.filter(m=>m.topic&&m.topic.startsWith('railway')).length"></div></div>
    <div class="stat"><div class="sl">From PC</div><div class="sv" x-text="msgs.filter(m=>m.topic&&m.topic.startsWith('pc')).length"></div></div>
  </div>

  <div class="panel">
    <div class="ph"><span>Live Messages (newest first)</span><span x-text="msgs.length+' msgs'"></span></div>
    <template x-if="msgs.length===0">
      <div class="empty">Waiting for MQTT messages...<br>Railway publisher sends every 5s to <b>railway/test</b></div>
    </template>
    <template x-for="(m,i) in msgs" :key="i">
      <div :class="'item '+(m.topic&&m.topic.startsWith('pc')?'pc':'')">
        <div :class="'tp '+(m.topic&&m.topic.startsWith('pc')?'pc':'')">
          <span x-text="m.topic"></span>
          <span :class="m.topic&&m.topic.startsWith('pc')?'src sp':'src sr'" x-text="m.topic&&m.topic.startsWith('pc')?'PC':'Railway'"></span>
        </div>
        <div class="pl" x-text="m.payload"></div>
        <div class="mt" x-text="m.timestamp+' | QoS '+m.qos"></div>
      </div>
    </template>
  </div>

  <script>
  function app(){
    return {
      msgs:[], ws:false, _ws:null,
      init(){
        document.getElementById('hostDisplay').textContent = location.hostname;
        this.connect();
      },
      connect(){
        const url = (location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws';
        console.log('WS connecting to', url);
        this._ws = new WebSocket(url);
        this._ws.onopen  = ()=>{ this.ws=true; };
        this._ws.onclose = ()=>{ this.ws=false; setTimeout(()=>this.connect(),3000); };
        this._ws.onerror = e=>console.error('WS err',e);
        this._ws.onmessage = e=>{
          const d=JSON.parse(e.data);
          this.msgs.unshift(d);
          if(this.msgs.length>50)this.msgs.pop();
        };
      }
    };
  }
  </script>
</body></html>
"""

# ── aiohttp HTTP + WebSocket server (single port) ─────────────────────────────
async def handle_root(request):
    return web.Response(text=HTML, content_type="text/html")

async def handle_ws(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    ws_clients.add(ws)
    print(f"[WS] client connected  total={len(ws_clients)}")
    # send history
    for m in message_history:
        try:
            await ws.send_str(json.dumps(m))
        except Exception:
            break
    async for msg in ws:
        if msg.type == aiohttp.WSMsgType.ERROR:
            break
    ws_clients.discard(ws)
    print(f"[WS] client gone       total={len(ws_clients)}")
    return ws

async def main():
    global _loop
    _loop = asyncio.get_running_loop()

    # 1. Start Mosquitto
    start_mosquitto()

    # 2. MQTT subscriber thread
    threading.Thread(target=start_mqtt_subscriber, daemon=True).start()

    # 3. Railway publisher thread (Subtask 2)
    threading.Thread(target=start_railway_publisher, daemon=True).start()

    # 4. aiohttp app
    aapp = web.Application()
    aapp.router.add_get("/",    handle_root)
    aapp.router.add_get("/ws",  handle_ws)

    runner = web.AppRunner(aapp)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", HTTP_PORT)
    await site.start()
    print(f"[HTTP+WS] listening on port {HTTP_PORT}")
    print(f"[INFO] Dashboard: http://0.0.0.0:{HTTP_PORT}/")
    print(f"[INFO] WebSocket: ws://0.0.0.0:{HTTP_PORT}/ws")

    await asyncio.Future()  # run forever

if __name__ == "__main__":
    print("=== MQTT Dashboard Server 20260706 ===")
    asyncio.run(main())
