# mqtt-server-20260706
MQTT server with Alpine.js dashboard for Railway deployment - test MQTT from PC to Railway

## PC Publisher Scripts

- **PC testing code.py** — PC1 publisher (VMBusiness). Publishes to topic `pc/test` on broker `hayabusa.proxy.rlwy.net:57802`.
- **pc2_pub.py** — PC2/VMNUC publisher. Publishes to topic `pc2/test` on the same broker.

## Broker

- External endpoint: `hayabusa.proxy.rlwy.net:57802`
- Dashboard shows live messages for `pc/test` (PC1) and `pc2/test` (PC2/VMNUC).
