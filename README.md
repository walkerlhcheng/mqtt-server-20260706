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

## Firewall Port Testing (Direct Connection — No TCP Proxy)

This section describes how to test whether MQTT ports are blocked by your local firewall or ISP, by connecting **directly** to the Railway server IP on the standard MQTT ports — bypassing the Railway TCP Proxy.

> **Purpose:** If your PC can reach `hayabusa.proxy.rlwy.net:57802` (Railway proxy) but you want to know whether standard MQTT ports are allowed through your firewall/ISP, run the test script below.

### Ports Tested

| Port | Protocol | Description |
|------|----------|-------------|
| 1883 | TCP | Standard unencrypted MQTT |
| 8883 | TCP | MQTT over TLS/SSL (encrypted) |
| 9001 | TCP | MQTT over WebSocket |

> **Note:** These ports are tested for TCP reachability only (socket connect). The Railway broker does **not** expose these ports directly — this test purely checks whether your **local network/firewall** allows outbound TCP on each port.

### How the Test Works

```
[PC / VMBusiness]
  runs: firewall_port_test_20260707v1.py
        |
        |  attempts TCP socket connect on each port
        v
[Target: hayabusa.proxy.rlwy.net]   (or any public host)
  port 1883  --> OPEN = firewall allows standard MQTT outbound
  port 8883  --> OPEN = firewall allows encrypted MQTT outbound
  port 9001  --> OPEN = firewall allows MQTT-WS outbound
        |
  BLOCKED = your local OS firewall / router / ISP is blocking that port
```

### Test Script

File: `firewall_port_test_20260707v1.py`  
Location: `C:\Users\chase\OneDrive\桌面\temp\`

```python
# firewall_port_test_20260707v1.py
# Tests outbound TCP reachability on MQTT standard ports
# WITHOUT using the Railway TCP proxy (port 57802)
# Purpose: detect which ports are blocked by local firewall or ISP

import socket
import datetime

HOST = 'hayabusa.proxy.rlwy.net'  # target host (Railway server)
PORTS = [
    (1883, 'MQTT unencrypted'),
    (8883, 'MQTT TLS/SSL'),
    (9001, 'MQTT WebSocket'),
]
TIMEOUT = 5  # seconds

print(f'Firewall Port Test - {datetime.datetime.now()}')
print(f'Target host: {HOST}')
print('-' * 50)

for port, desc in PORTS:
    try:
        with socket.create_connection((HOST, port), timeout=TIMEOUT) as s:
            print(f'[OPEN]    Port {port:5d} ({desc}) -- TCP connect SUCCESS')
    except ConnectionRefusedError:
        print(f'[CLOSED]  Port {port:5d} ({desc}) -- Connection refused (port closed on server)')
    except socket.timeout:
        print(f'[BLOCKED] Port {port:5d} ({desc}) -- Timed out (likely blocked by firewall/ISP)')
    except Exception as e:
        print(f'[ERROR]   Port {port:5d} ({desc}) -- {e}')

print('-' * 50)
print('Done.')
```

### Interpreting Results

| Result | Meaning |
|--------|---------|
| `[OPEN]` | Outbound TCP allowed — port not blocked |
| `[BLOCKED]` | Timeout — firewall/ISP is silently dropping the packets |
| `[CLOSED]` | Server refused — firewall passed it but server not listening |

### Run Instructions (VMBusiness PC)

```bash
cd C:\Users\chase\OneDrive\桌面\temp
python firewall_port_test_20260707v1.py
```
