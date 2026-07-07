import paho.mqtt.client as m
import time

c = m.Client()
c.connect('hayabusa.proxy.rlwy.net', 57802)
c.loop_start()

for i in range(1, 11):
    c.publish('pc/test', 'Hello from VMBusiness PC1! Msg #' + str(i))
    time.sleep(0.5)

time.sleep(1)
c.loop_stop()
