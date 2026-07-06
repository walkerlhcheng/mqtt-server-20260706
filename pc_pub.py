import paho.mqtt.client as mqtt
import time
from datetime import datetime

BROKER = 'hayabusa.proxy.rlwy.net'
PORT = 24029
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
time.sleep(4)

print(f'[PC1] connected={connected[0]}')
for i in range(20):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    msg = f'Hello from VMBusiness PC1! Msg #{i+1} at {ts}'
    result = client.publish(TOPIC, msg)
    result.wait_for_publish()
    print(f'[PC1] Sent #{i+1} rc={result.rc}')
    time.sleep(5)

client.loop_stop()
print('[PC1] Done')
