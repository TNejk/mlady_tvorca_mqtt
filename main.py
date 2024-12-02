import json
import datetime
import paho.mqtt.client as mqtt
import psycopg2

# MQTT connection parameters
broker = "193.87.172.144"
mqtt_port = 1983

# Topics to subscribe to
topics = [
    ("Tunel_1/esp32/TeplotaVzduchu", 1),
    ("Tunel_1/esp32/VlhkostVzduchu", 1),
    ("Tunel_1/esp32/TlakVzduchu", 1),
    ("Tunel_1/esp32/AeroVrtula", 1)
]

# Database connection parameters
db = "postgres"
user = "postgres"
host = "localhost"
password = ""  # Insert your password
db_port = 5433  # Outside port

def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        with open("logs.txt", "a") as f:
            f.write(f"{datetime.datetime.now()}; Connected with result code {reason_code}\n")
        global mqtt_connected
        mqtt_connected = True
    else:
        with open("logs.txt", "a") as f:
            f.write(f"{datetime.datetime.now()}; Connection failed with result code {reason_code}\n")


def on_message(client, userdata, msg, properties=None):
    global conn
    try:
        conn = db_connect()
        with conn.cursor() as cursor:
            recieved = msg.payload.decode("utf-8")
            m_decoded = json.loads(recieved)
            data = json.dumps(m_decoded)
            topic = msg.topic
            write = f"INSERT INTO mqtt_data (topic, data) VALUES ('{topic}', '{data}')"
            cursor.execute(write)
            conn.commit()
    except (json.JSONDecodeError, IOError) as e:
        with open("error_logs.txt", "a") as error_file:
            error_file.write(f"{datetime.datetime.now()}; Error: {str(e)}\n")
    finally:
        conn.close()


def wait_for_mqtt():
    global mqtt_connected
    while not mqtt_connected:
        pass
    client.subscribe(topics)


def db_connect():
    try:
        conn = psycopg2.connect(database=db, user=user, password=password, host=host, port=db_port)
        with open("logs.txt", "a") as f:
            f.write(f"{datetime.datetime.now()}; Connected to the database!\n")
        return conn
    except psycopg2.Error as e:
        with open("logs.txt", "a") as f:
            f.write(f"{datetime.datetime.now()}; Connection failed with result code {str(e)}\n")
        return None

# MQTT connect
mqtt_connected = False
client = mqtt.Client(client_id="localny_sniffer", reconnect_on_failure=True)
client.on_connect = on_connect
client.on_message = on_message
client.connect(broker, mqtt_port)
client.loop_start()
wait_for_mqtt()

try:
    while True:
        pass
except KeyboardInterrupt:
    print("Exiting")
    client.disconnect()
    client.loop_stop()