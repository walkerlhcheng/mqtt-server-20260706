import socket
import datetime

# Firewall Port Test for MQTT Broker
# Target: hayabusa.proxy.rlwy.net
# Tests ports: 57802 (Railway MQTT), 1883, 8883, 9001
# Version: 20260707v1

HOST = 'hayabusa.proxy.rlwy.net'
PORTS = [57802, 1883, 8883, 9001]
TIMEOUT = 5
LOG_FILE = 'firewall_port_test_20260707v1_result.txt'

results = []
results.append('Firewall Port Test - ' + str(datetime.datetime.now()))
results.append('Target Host: ' + HOST)
results.append('-' * 50)

for port in PORTS:
    try:
        sock = socket.create_connection((HOST, port), timeout=TIMEOUT)
        sock.close()
        line = 'Port ' + str(port) + ': OPEN'
    except ConnectionRefusedError:
        line = 'Port ' + str(port) + ': CLOSED (connection refused)'
    except socket.timeout:
        line = 'Port ' + str(port) + ': BLOCKED (timeout after ' + str(TIMEOUT) + 's)'
    except Exception as e:
        line = 'Port ' + str(port) + ': ERROR - ' + str(e)
    results.append(line)
    print(line)

results.append('-' * 50)
results.append('Test complete.')

with open(LOG_FILE, 'w') as f:
    f.write('\n'.join(results) + '\n')

print('Results saved to: ' + LOG_FILE)
