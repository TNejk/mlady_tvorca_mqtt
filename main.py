import os
import json
import datetime
import psycopg2
import paho.mqtt.client as mqtt
import signal

# Configuration
broker = "193.87.172.144"
mqtt_port = 1983
db = os.getenv("DB_NAME", "postgres")
user = os.getenv("DB_USER", "postgres")
password = os.getenv("DB_PASSWORD", "")
host = os.getenv("DB_HOST", "localhost")
db_port = os.getenv("DB_PORT", 5433)

topics = [
 ("Tunel_1/esp32/TeplotaVzduchu", 1),
 ("Tunel_1/esp32/VlhkostVzduchu", 1),
 ("Tunel_1/esp32/TlakVzduchu", 1),
 ("Tunel_1/esp32/AeroVrtula", 1)
]

conn = None

def db_connect():
 global conn
 if conn is None or conn.closed != 0:
     try:
         conn = psycopg2.connect(database=db, user=user, password=password, host=host, port=db_port)
         print("Database connected")
     except psycopg2.Error as e:
         print(f"Database connection failed: {str(e)}")
 return conn

def cleanup(sig, frame):
 if conn:
     conn.close()
 client.disconnect()
 client.loop_stop()
 print("Clean exit")
 exit(0)

def on_connect(client, userdata, flags, reason_code, properties=None):
 print(f"Connected with result code {reason_code}")
 if reason_code == 0:
     client.subscribe(topics)

def on_message(client, userdata, msg, properties=None):
 try:
     conn = db_connect()
     with conn.cursor() as cursor:
         received = msg.payload.decode("utf-8")
         data = json.dumps(json.loads(received))
         cursor.execute("INSERT INTO mqtt_data (topic, data) VALUES (%s, %s)", (msg.topic, data))
         conn.commit()
 except Exception as e:
     print(f"Error processing message: {str(e)}")

# Signal Handling
signal.signal(signal.SIGINT, cleanup)
signal.signal(signal.SIGTERM, cleanup)

# MQTT Client
client = mqtt.Client(client_id="localny_sniffer", reconnect_on_failure=True)
client.on_connect = on_connect
client.on_message = on_message

client.connect(broker, mqtt_port)
client.loop_start()

# Main Loop
try:
 while True:
     pass
except KeyboardInterrupt:
 cleanup(None, None)