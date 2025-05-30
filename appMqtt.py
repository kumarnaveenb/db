import os

import sqlite3
import threading
import time
import json
import socket # For network connectivity check

# Install this library if you haven't already:
# pip install paho-mqtt==1.3.1 # Use an older version compatible with Python 2.7
devId = "40a36bc38f6f"
location = "Sarasbaugh"

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print "Error: 'paho-mqtt' library not found."
    print "Please install it using: pip install paho-mqtt==1.3.1"
    exit(1)

# --- Configuration ---
# MQTT Settings
MQTT_BROKER_HOST = 'mqtt.kosine.tech'  # e.g., 'broker.hivemq.com' or '192.168.1.100'
MQTT_BROKER_PORT = 1883
MQTT_TOPIC_BASE = 'kosine/smartmileNew/assets/ups/data/'+devId # Base topic for all sensor data
MQTT_CLIENT_ID = location # Unique client ID for this script
MQTT_USERNAME = 'kosine' # Set to 'your_username' if authentication is required
MQTT_PASSWORD = 'kosine@210620' # Set to 'your_password' if authentication is required
MQTT_KEEPALIVE = 60 # seconds

# Data Buffering Settings (must match the other script)
DB_FILE = 'sensor_data.db'
APP_PATH = '/root/work/ups/db' # Directory where your DB file is located

SEND_INTERVAL_SECONDS = 10 # How often to try sending data

# Network Connectivity Check
NETWORK_CHECK_HOST =  MQTT_BROKER_HOST # Google DNS server, good for network check
NETWORK_CHECK_PORT = MQTT_BROKER_PORT # DNS port

# --- Global Flags and Locks ---
network_online = False
mqtt_connected = False
db_lock = threading.Lock() # To prevent race conditions when accessing the database

# --- Database Management ---
def get_single_reading_from_db():
    """Retrieves the oldest single buffered reading from the database."""
    with db_lock:
        conn = None
        try:
            # Connect to the database file in its specified path
            conn = sqlite3.connect(APP_PATH + '/' + DB_FILE)
            cursor = conn.cursor()
            # Select the oldest record based on timestamp
            cursor.execute('SELECT id, timestamp, acVoltage, acStatus, upsVoltage, upsStatus FROM sensor_readings ORDER BY timestamp ASC LIMIT 1')
            
            row = cursor.fetchone() # Fetch only one record

            if row:
                reading = {
                    'id': row[0],
                    'timestamp': row[1],
                    'acVoltage': row[2],
                    'acStatus': row[3],
                    'upsVoltage': row[4],
                    'upsStatus': row[5]
                }
                return reading
            else:
                return None # No records found
        except sqlite3.Error, e:
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
            # Connect to the database file in its specified path
            conn = sqlite3.connect(APP_PATH + '/' + DB_FILE)
            cursor = conn.cursor()
            cursor.execute('DELETE FROM sensor_readings WHERE id = ?', (reading_id,))
            conn.commit()
            print "[MQTT Sender] Deleted reading ID {0} from DB.".format(reading_id)
            return True
        except sqlite3.Error, e:
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
        # Try to connect to a well-known host (e.g., Google DNS)
        # This is a quick check, not a guarantee of full internet access
        socket.create_connection((NETWORK_CHECK_HOST, NETWORK_CHECK_PORT), timeout=3)
        if not network_online:
            print "[MQTT Sender] Network is online."
        network_online = True
    except socket.error, e: # Python 2 socket exception syntax
        if network_online:
            print "[MQTT Sender] Network is offline: {0}".format(e)
        network_online = False
    except Exception, e:
        print "[MQTT Sender] Unexpected error during network check: {0}".format(e)
        network_online = False
    return network_online

# --- MQTT Client ---
class MqttHandler:
    def __init__(self, broker_host, broker_port, client_id, topic_base, username=None, password=None):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client_id = client_id
        self.topic_base = topic_base
        self.username = username
        self.password = password
        self.client = mqtt.Client(client_id=self.client_id)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        if self.username and self.password:
            self.client.username_pw_set(self.username, self.password)
        self.connect_attempted = False

    def _on_connect(self, client, userdata, flags, rc):
        global mqtt_connected
        if rc == 0:
            print "[MQTT Sender] MQTT Connected successfully."
            mqtt_connected = True
        else:
            print "[MQTT Sender] MQTT Connection failed with code {0}".format(rc)
            mqtt_connected = False

    def _on_disconnect(self, client, userdata, rc):
        global mqtt_connected
        print "[MQTT Sender] MQTT Disconnected with code {0}".format(rc)
        mqtt_connected = False

    def connect(self):
        """Attempts to connect to the MQTT broker."""
        if self.client.is_connected():
            return True
        print "[MQTT Sender] Attempting to connect to MQTT broker: {0}:{1}".format(self.broker_host, self.broker_port)
        try:
            self.client.connect(self.broker_host, self.broker_port, MQTT_KEEPALIVE)
            self.client.loop_start() # Start a background thread to handle network traffic
            self.connect_attempted = True
            return True
        except Exception, e:
            print "[MQTT Sender] MQTT connection error: {0}".format(e)
            self.connect_attempted = False
            return False

    def publish_reading(self, reading):
        """Publishes a single sensor reading to MQTT."""
        if not self.client.is_connected():
            print "[MQTT Sender] MQTT client not connected. Cannot publish."
            return False

        # Use a generic topic for all sensor data, or create sub-topics if needed
        topic = self.topic_base
        
        # Capture pub Time
        pubTime = time.time()

        # Prepare payload from the reading dictionary
        payload = {
            "devId":devId,
            "inputVoltage": reading['acVoltage'],
            "acStatus": reading['acStatus'],
            "outputVoltage": reading['upsVoltage'],
            "upsStatus": reading['upsStatus'],
            "location": location,
            "timestamp": reading['timestamp'],
            "pubTime": pubTime
        }
        
        try:
            self.client.publish(topic, json.dumps(payload), qos=1) # QoS 1 for guaranteed delivery
            print "[MQTT Sender] Published to {0}: {1}".format(topic, payload)
            return True
        except Exception, e:
            print "[MQTT Sender] Error publishing to MQTT: {0}. Data will remain buffered.".format(e)
            return False

# --- Main Application Logic ---
def main():
    print "[MQTT Sender] Starting MQTT sender and SQLite reader..."

    mqtt_handler = MqttHandler(
        MQTT_BROKER_HOST, MQTT_BROKER_PORT, MQTT_CLIENT_ID, MQTT_TOPIC_BASE,
        MQTT_USERNAME, MQTT_PASSWORD
    )

    try: # Outer try block to encompass the while loop and ensure final cleanup
        while True:
            try: # Inner try block for exceptions within each loop iteration
                check_network_connectivity()

                if network_online:
                    if not mqtt_connected:
                        mqtt_handler.connect()
                    
                    if mqtt_connected:
                        # Only pick up data if online and MQTT connected
                        reading_to_send = get_single_reading_from_db()
                        
                        if reading_to_send:
                            print "[MQTT Sender] Found buffered data (ID: {0}). Attempting to send...".format(reading_to_send['id'])
                            if mqtt_handler.publish_reading(reading_to_send):
                                # If successfully published, delete from DB
                                delete_reading_from_db(reading_to_send['id'])
                            else:
                                print "[MQTT Sender] Failed to publish data. Will retry on next cycle."
                        else:
                            print "[MQTT Sender] No buffered data to send."
                    else:
                        print "[MQTT Sender] Waiting for MQTT connection..."
                else:
                    print "[MQTT Sender] Network is offline. Not checking database or attempting MQTT connection."

                time.sleep(SEND_INTERVAL_SECONDS)

            except KeyboardInterrupt:
                print "\n[MQTT Sender] KeyboardInterrupt detected. Shutting down gracefully..."
                break # Exit the while loop
            except SystemExit: # Catches signals like SIGTERM from init.d stop
                print "\n[MQTT Sender] SystemExit signal received. Shutting down gracefully..."
                break # Exit the while loop
            except Exception, e:
                print "\n[MQTT Sender] An unexpected error occurred in main loop: {0}".format(e)
                time.sleep(SEND_INTERVAL_SECONDS) # Wait before retrying

    finally: # This finally block is now correctly paired with the outer try
        # --- Clean up resources ---
        if mqtt_handler.client.is_connected():
            try:
                mqtt_handler.client.loop_stop() # Stop the MQTT loop thread
                mqtt_handler.client.disconnect()
                print "[MQTT Sender] MQTT client disconnected."
            except Exception, e:
                print "[MQTT Sender] Error during MQTT client shutdown: {0}".format(e)
        
        print "[MQTT Sender] Application terminated."

if __name__ == "__main__":
    main()
