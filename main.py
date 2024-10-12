import json
import datetime
import paho.mqtt.client as mqtt
import psycopg2

# mqtt connection params
broker = "localhost"
mqtt_port = 1983

# topics
topics = [("Tunel_1/esp32/TeplotaVzduchu",1),("Tunel_1/esp32/VlhkostVzduchu",1),("Tunel_1/esp32/TlakVzduchu",1),("Tunel_1/esp32/AeroVrtula",1)]
'''
temp = # 23.1 (°C)
humi = # 65.5 (%)
pres = # 980.4 (hPa)
aero = 1.54 (m/s)
'''

# db connection params
db_connection = None
db = "postgres"
user = "postgres"
host = "localhost"
password = ""       # insert your password
db_port = 5433      # outside port

def on_connect(client, userdata, flags, reasonCode):
    if reasonCode == 0:
        with open("logs.txt", "a") as f:
            f.write(f"{datetime.datetime.now()}; Connected with result code {reasonCode}\n")
        global mqtt_connected
        mqtt_connected = True
    else:
        with open("logs.txt", "a") as f:
            f.write(f"{datetime.datetime.now()}; Connection failed with result code {reasonCode}\n")

def on_message(client, userdata, msg):
    try:
        data = msg.payload.decode("utf-8")
        m_decoded = json.loads(data)
        with open("temp_readings.txt", "a") as f:
            f.write(json.dumps(m_decoded) + "\n")
    except (json.JSONDecodeError, IOError) as e:
        with open("error_logs.txt", "a") as error_file:
            error_file.write(f"{datetime.datetime.now()}; Error: {str(e)}\n")

def wait_for_mqtt():
    global mqtt_connected
    while not mqtt_connected:
        pass
    client.subscribe(topics)

def db_connect():
    global db_connection
    try:
        db_connection = psycopg2.connect(database=db, user=user, password=password, host=host, port=db_port)
    except psycopg2.Error as e:
        with open("logs.txt", "a") as f:
            f.write(f"{datetime.datetime.now()};Connected failed with result code "+str(e))

# db connect
db_connect()

# mqtt connect
mqtt_connected = False
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(broker, mqtt_port)

client.loop_start()
wait_for_mqtt()

try:
  while True:
    pass
except KeyboardInterrupt:
  print("exiting")
  client.disconnect()
  client.loop_stop()