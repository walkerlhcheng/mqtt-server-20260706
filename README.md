# mqtt-server-20260706
MQTT server with Alpine.js dashboard for Railway deployment - test MQTT from PC to Railway

## PC Publisher Scripts

- **PC testing code.py** — PC1 publisher (VMBusiness). Publishes to topic `pc/test` on broker `hayabusa.proxy.rlwy.net:57802`.
- **pc2_pub.py** — PC2/VMNUC publisher. Publishes to topic `pc2/test` on the same broker.

## Broker

- External endpoint: `hayabusa.proxy.rlwy.net:57802`
- Dashboard shows live messages for `pc/test` (PC1) and `pc2/test` (PC2/VMNUC).

## Connection Flow (PC → Railway MQTT Broker)

This diagram shows how an MQTT publish message travels from a PC to the broker running on Railway, and where the firewall/proxy boundary sits.

```
[PC / VMBusiness]
  runs: PC testing code.py
  connects to: hayabusa.proxy.rlwy.net:57802  (TCP)
        |
        |  <-- Internet / your local network firewall here
        |      The PC must allow outbound TCP on port 57802.
        |      No inbound port opening needed on the PC side.
        v
[Railway TCP Proxy]  <-- "firewall boundary" / public entry point
  host: hayabusa.proxy.rlwy.net
  port: 57802  (public, assigned by Railway)
  Role: forwards raw TCP traffic into the Railway private network
        |
        |  (Railway internal network -- not reachable from outside)
        v
[Railway Container: mqtt-server-20260706]
  MQTT Broker (amqtt, embedded in app.py)
  listens on: 0.0.0.0:1884  (TCP, internal only)  <-- actual broker port
  listens on: 127.0.0.1:1885  (loopback, for publisher.py inside same container)
  listens on: 0.0.0.0:9001  (WebSocket, for Alpine.js dashboard)
        |
        v
  app.py receives the PUBLISH, broadcasts to WebSocket clients
        |
        v
[Browser Dashboard]  (Alpine.js + WebSocket on port 9001)
  shows live messages on pc/test and pc2/test
```

### Where is the firewall?

- **PC side:** your local OS/router firewall — only **outbound** TCP to port `57802` needs to be open. No inbound rule required.
- **Railway side:** Railway's TCP Proxy acts as the public gateway. Port `57802` is the only externally exposed port. The internal broker port `1884` is **not** directly reachable from the internet.

### Where is the broker?

- The MQTT broker (`amqtt`) runs **inside the Railway container** (`app.py`), bound to internal port `1884`.
- External clients (PCs) never connect to port `1884` directly — they always go through the Railway TCP Proxy at port `57802`.
- `publisher.py` (the server-side publisher) connects via the internal loopback port `1885` to avoid going through the proxy.
