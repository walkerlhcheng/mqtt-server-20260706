import paho.mqtt.client as mqtt
import time
from datetime import datetime

BROKER = 'hayabusa.proxy.rlwy.net'
PORT = 57802
TOPIC = 'pc/test'

connected = [False]

def on_connect(c, u, f, rc):
    if rc == 0:
        connected[0] = True
        print('[PC1] Connected to Railway MQTT broker!')
        c.subscribe(TOPIC)
    else:
        print(f'[PC1] Connection failed rc={rc}')

client = mqtt.Client(client_id='pc1_vmbusiness')
client.on_connect = on_connect
print(f'[PC1] Connecting to {BROKER}:{PORT}...')
client.connect(BROKER, PORT, 60)
client.loop_start()

# Wait up to 10 seconds for connection
for _ in range(20):
    if connected[0]:
        break
    time.sleep(0.5)

print(f'[PC1] connected={connected[0]}')
if not connected[0]:
    print('[PC1] ERROR: Could not connect to broker!')
    client.loop_stop()
    exit(1)

for i in range(20):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    msg = f'Hello from VMBusiness PC1! Msg #{i+1} at {ts}'
    result = client.publish(TOPIC, msg)
    result.wait_for_publish()
    print(f'[PC1] Sent #{i+1} rc={result.rc}')
    time.sleep(5)

client.loop_stop()
print('[PC1] Done')
