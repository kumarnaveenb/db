import os
import sqlite3
import threading
import time
import json
import socket
import random

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print "Error: 'paho-mqtt' library not found."
    print "Please install it using: pip install paho-mqtt==1.3.1"
    exit(1)

# --- Configuration ---
# Device Settings
devId = "40A36BC38498"
location = "Wagheshwar"
ip = "172.16.4.46"

# MQTT Settings
MQTT_BROKER_HOST = 'mqtt.kosine.tech'
MQTT_BROKER_PORT = 1883
MQTT_TOPIC_BASE = 'kosine/smartmileNew/assets/ups/data/'+devId
MQTT_CLIENT_ID = location+str(random.randint(0, 1000))
MQTT_USERNAME = 'kosine'
MQTT_PASSWORD = 'kosine@210620'
MQTT_KEEPALIVE = 60

# Data Buffering Settings
DB_FILE = 'sensor_data.db'
APP_PATH = '/root/work/ups/db'
SEND_INTERVAL_SECONDS = 10

# Network Connectivity Check
NETWORK_CHECK_HOST = MQTT_BROKER_HOST
NETWORK_CHECK_PORT = MQTT_BROKER_PORT

# --- Global Flags and Locks ---
network_online = False
mqtt_connected = False
db_lock = threading.Lock()

# --- Database Management ---
def get_single_reading_from_db():
    """Retrieves the oldest single buffered reading from the database."""
    with db_lock:
        conn = None
        try:
            conn = sqlite3.connect(os.path.join(APP_PATH, DB_FILE))
            cursor = conn.cursor()
            cursor.execute('SELECT id, timestamp, acVoltage, acStatus, upsVoltage, upsStatus FROM sensor_readings ORDER BY timestamp ASC LIMIT 1')
            row = cursor.fetchone()

            if row:
                return {
                    'id': row[0],
                    'timestamp': row[1],
                    'acVoltage': row[2],
                    'acStatus': row[3],
                    'upsVoltage': row[4],
                    'upsStatus': row[5]
                }
            return None
        except sqlite3.Error as e:
            print "[MQTT Sender] Error retrieving from database: {0}".format(e)
            return None
        finally:
            if conn:
                conn.close()

def delete_reading_from_db(reading_id):
    """Deletes a specific reading from the database by its ID."""
    with db_lock:
        conn = None
        try:
            conn = sqlite3.connect(os.path.join(APP_PATH, DB_FILE))
            cursor = conn.cursor()
            cursor.execute('DELETE FROM sensor_readings WHERE id = ?', (reading_id,))
            conn.commit()
            print "[MQTT Sender] Deleted reading ID {0} from DB.".format(reading_id)
            return True
        except sqlite3.Error as e:
            print "[MQTT Sender] Error deleting from database (ID: {0}): {1}".format(reading_id, e)
            return False
        finally:
            if conn:
                conn.close()

# --- Network Connectivity Check ---
def check_network_connectivity():
    """Checks if the device has network connectivity."""
    global network_online
    try:
        sock = socket.create_connection((NETWORK_CHECK_HOST, NETWORK_CHECK_PORT), timeout=3)
        sock.close()
        if not network_online:
            print "[MQTT Sender] Network is online."
        network_online = True
        return True
    except Exception as e:
        if network_online:
            print "[MQTT Sender] Network is offline: {0}".format(e)
        network_online = False
        return False

# --- MQTT Client ---
class MqttHandler:
    def __init__(self, broker_host, broker_port, client_id, topic_base, username=None, password=None):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client_id = client_id
        self.topic_base = topic_base
        self.username = username
        self.password = password
        
        # Initialize MQTT client
        self.client = mqtt.Client(client_id=self.client_id)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_socket_open = self._on_socket_open
        self.client.on_socket_close = self._on_socket_close
        
        if self.username and self.password:
            self.client.username_pw_set(self.username, self.password)
            
        # Enable automatic reconnect
        self.client.reconnect_delay_set(min_delay=1, max_delay=120)
        
        # Connection state
        self.connection_in_progress = False
        self.last_connect_attempt = 0

    def _on_socket_open(self, client, userdata, sock):
        """Called when a socket is opened"""
        try:
            # Set TCP keepalive options
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            # For Linux
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 10)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
            print "[MQTT Sender] Socket options set successfully"
        except Exception as e:
            print "[MQTT Sender] Error setting socket options: {0}".format(e)

    def _on_socket_close(self, client, userdata, sock):
        """Called when a socket is closed"""
        print "[MQTT Sender] Socket closed"

    def _on_connect(self, client, userdata, flags, rc):
        global mqtt_connected
        if rc == 0:
            print "[MQTT Sender] MQTT Connected successfully to {0}:{1}".format(self.broker_host, self.broker_port)
            mqtt_connected = True
            self.connection_in_progress = False
        else:
            print "[MQTT Sender] MQTT Connection failed with code {0}".format(rc)
            mqtt_connected = False
            self.connection_in_progress = False

    def _on_disconnect(self, client, userdata, rc):
        global mqtt_connected
        print "[MQTT Sender] MQTT Disconnected with code {0}".format(rc)
        mqtt_connected = False
        self.connection_in_progress = False

    def connect(self):
        """Attempts to connect to the MQTT broker."""
        if self.connection_in_progress:
            return False
            
        if self.client.is_connected():
            return True
            
        current_time = time.time()
        if current_time - self.last_connect_attempt < 5:  # Limit connection attempts
            return False
            
        print "[MQTT Sender] Attempting to connect to MQTT broker at {0}:{1}...".format(
            self.broker_host, self.broker_port)
        
        try:
            self.connection_in_progress = True
            self.last_connect_attempt = current_time
            self.client.connect_async(self.broker_host, self.broker_port, keepalive=MQTT_KEEPALIVE)
            self.client.loop_start()
            return True
        except Exception as e:
            print "[MQTT Sender] MQTT connection error: {0}".format(e)
            self.connection_in_progress = False
            return False

    def publish_reading(self, reading):
        """Publishes a single sensor reading to MQTT."""
        if not self.client.is_connected():
            print "[MQTT Sender] MQTT client not connected. Cannot publish."
            return False

        topic = self.topic_base
        pubTime = int(time.time())

        payload = {
            "devId": devId,
            "inputVoltage": reading['acVoltage'],
            "acStatus": reading['acStatus'],
            "outputVoltage": reading['upsVoltage'],
            "upsStatus": reading['upsStatus'],
            "location": location,
            "timestamp": reading['timestamp'],
            "pubTime": pubTime
        }
        
        try:
            info = self.client.publish(topic, json.dumps(payload), qos=1)
            if info.rc == mqtt.MQTT_ERR_SUCCESS:
                print "[MQTT Sender] Published to {0}: {1}".format(topic, payload)
                return True
            else:
                print "[MQTT Sender] Failed to publish (rc: {0})".format(info.rc)
                return False
        except Exception as e:
            print "[MQTT Sender] Error publishing to MQTT: {0}. Data will remain buffered.".format(e)
            return False

# --- Main Application Logic ---
def main():
    print "[MQTT Sender] Starting MQTT sender and SQLite reader..."

    mqtt_handler = MqttHandler(
        MQTT_BROKER_HOST, MQTT_BROKER_PORT, MQTT_CLIENT_ID, MQTT_TOPIC_BASE,
        MQTT_USERNAME, MQTT_PASSWORD
    )

    # Start MQTT connection
    mqtt_handler.connect()

    try:
        while True:
            try:
                # Check network connectivity
                network_ok = check_network_connectivity()
                
                if network_ok:
                    # Maintain MQTT connection
                    if not mqtt_connected and not mqtt_handler.connection_in_progress:
                        mqtt_handler.connect()
                    
                    # Process data if connected
                    if mqtt_connected:
                        reading_to_send = get_single_reading_from_db()
                        
                        if reading_to_send:
                            print "[MQTT Sender] Found buffered data (ID: {0}). Attempting to send...".format(reading_to_send['id'])
                            if mqtt_handler.publish_reading(reading_to_send):
                                delete_reading_from_db(reading_to_send['id'])
                            else:
                                print "[MQTT Sender] Failed to publish data. Will retry on next cycle."
                        else:
                            print "[MQTT Sender] No buffered data to send."
                else:
                    print "[MQTT Sender] Network is offline. Not attempting MQTT connection."

                time.sleep(SEND_INTERVAL_SECONDS)

            except KeyboardInterrupt:
                print "\n[MQTT Sender] KeyboardInterrupt detected. Shutting down gracefully..."
                break
            except Exception as e:
                print "\n[MQTT Sender] An unexpected error occurred in main loop: {0}".format(e)
                time.sleep(SEND_INTERVAL_SECONDS)

    finally:
        if mqtt_handler.client.is_connected():
            try:
                mqtt_handler.client.loop_stop()
                mqtt_handler.client.disconnect()
                print "[MQTT Sender] MQTT client disconnected."
            except Exception as e:
                print "[MQTT Sender] Error during MQTT client shutdown: {0}".format(e)
        
        print "[MQTT Sender] Application terminated."

if __name__ == "__main__":
    main()