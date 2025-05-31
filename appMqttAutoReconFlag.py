# -*- coding: utf-8 -*-
import os
import sqlite3
import threading
import time
import json
import socket # For network connectivity check
import sys # Added for sys.exit()
import random

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print "Error: 'paho-mqtt' library not found."
    print "Please install it using: pip install paho-mqtt==1.3.1"
    sys.exit(1) # Exit if essential library is missing

# --- Configuration ---
# Device Settings
devId = "40A36BC38E91"
location = "Office"

# MQTT Settings
MQTT_BROKER_HOST = 'mqtt.kosine.tech'
MQTT_BROKER_PORT = 1883
MQTT_TOPIC_BASE = 'kosine/smartmileNew/assets/ups/data/'+devId
MQTT_CLIENT_ID = location+str(random.randint(0, 1000)) # Unique client ID for this script
MQTT_USERNAME = 'kosine' # Set to 'your_username' if authentication is required
MQTT_PASSWORD = 'kosine@210620' # Set to 'your_password' if authentication is required
MQTT_KEEPALIVE = 60 # seconds

# Data Buffering Settings
DB_FILE = 'sensor_data.db'
APP_PATH = '/root/work/ups/db' # Directory where your DB file is located

SEND_INTERVAL_SECONDS = 10 # How often to try sending data (and check connection status)

# Network Connectivity Check
NETWORK_CHECK_HOST = MQTT_BROKER_HOST # Using broker host for network check
NETWORK_CHECK_PORT = MQTT_BROKER_PORT # Using broker port for network check

# Connection Timeout/Restart Settings
# Number of SEND_INTERVAL_SECONDS cycles to wait before exiting script for restart
# E.g., if SEND_INTERVAL_SECONDS is 60 (1 min), 10 cycles = 10 minutes.
MAX_CONSECUTIVE_WAIT_CYCLES = 10

# --- Reconnection Parameters (for _on_disconnect's custom loop) ---
# These parameters control the exponential backoff for reconnection attempts
# initiated within the _on_disconnect callback.
FIRST_RECONNECT_DELAY = 1 # Initial delay in seconds before first reconnect attempt
RECONNECT_RATE = 2 # Multiplier for the delay (e.g., 1, 2, 4, 8, ...)
MAX_RECONNECT_COUNT = 12 # Maximum number of reconnection attempts before giving up
MAX_RECONNECT_DELAY = 60 # Cap on the maximum delay between reconnect attempts in seconds

# --- Global Flags and Locks ---
network_online = False
mqtt_connected = False
db_lock = threading.Lock() # To prevent race conditions when accessing the database
FLAG_EXIT = False # New flag to signal the main loop to terminate gracefully

# --- Database Management ---
def get_single_reading_from_db():
    """Retrieves the oldest single buffered reading from the database."""
    with db_lock:
        conn = None
        try:
            # Connect to the database file in its specified path
            conn = sqlite3.connect(os.path.join(APP_PATH, DB_FILE))
            cursor = conn.cursor()
            # Select the oldest record based on timestamp
            cursor.execute('SELECT id, timestamp, acVoltage, acStatus, upsVoltage, upsStatus FROM sensor_readings ORDER BY timestamp ASC LIMIT 1')

            row = cursor.fetchone() # Fetch only one record

            if row:
                return {
                    'id': row[0],
                    'timestamp': row[1],
                    'acVoltage': row[2],
                    'acStatus': row[3],
                    'upsVoltage': row[4],
                    'upsStatus': row[5]
                }
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
            conn = sqlite3.connect(os.path.join(APP_PATH, DB_FILE))
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
        # Try to create a connection to the MQTT broker host/port
        sock = socket.create_connection((NETWORK_CHECK_HOST, NETWORK_CHECK_PORT), timeout=3)
        sock.close() # Close the socket immediately
        if not network_online: # Only print if state changes
            print "[MQTT Sender] Network is online."
        network_online = True
    except socket.error, e: # Python 2 socket exception syntax
        if network_online: # Only print if state changes
            print "[MQTT Sender] Network is offline: {0}".format(e)
        network_online = False
    except Exception, e: # Catch any other unexpected errors during check
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

        # Initialize MQTT client
        self.client = mqtt.Client(client_id=self.client_id)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

        # Set username and password if provided
        if self.username and self.password:
            self.client.username_pw_set(self.username, self.password)

        # IMPORTANT: Removed Paho's internal reconnect_delay_set, as we're
        # implementing custom reconnection logic in _on_disconnect.
        # If you uncomment this, it will be redundant with the custom loop.
        # self.client.reconnect_delay_set(min_delay=1, max_delay=120) 

        # Start the background loop thread immediately on initialization.
        # This thread handles network traffic, keeping the connection alive and
        # managing communication. Our custom _on_disconnect will tell it to reconnect.
        self.client.loop_start() 
        print "[MQTT Sender] Paho MQTT client loop started."

    def _on_connect(self, client, userdata, flags, rc):
        """Callback for when the client receives a CONNACK response from the server."""
        global mqtt_connected, FLAG_EXIT
        if rc == 0:
            print "[MQTT Sender] MQTT Connected successfully to {0}:{1}".format(self.broker_host, self.broker_port)
            mqtt_connected = True
        else:
            # Important: rc values indicate specific connection failures
            # e.g., 1: Protocol error, 2: Client ID rejected, 3: Server unavailable,
            #       4: Bad username/password, 5: Not authorized
            print "[MQTT Sender] MQTT Connection failed with code {0} - {1}".format(rc, mqtt.connack_string(rc))
            mqtt_connected = False
            # If authentication/authorization fails (rc 4 or 5), trigger a critical exit.
            if rc in [4, 5]:
                print "[MQTT Sender] CRITICAL: Authentication/Authorization failure ({0}). Exiting application.".format(rc)
                FLAG_EXIT = True # Signal main loop to terminate
                sys.exit(1) # Force exit if unrecoverable

    def _on_disconnect(self, client, userdata, rc):
        """Callback for when the client disconnects from the server."""
        global mqtt_connected, FLAG_EXIT
        print "[MQTT Sender] MQTT Disconnected with code {0}. Initiating reconnect attempts.".format(rc)
        mqtt_connected = False # Update global status immediately

        # --- Custom Reconnection Logic with Exponential Backoff ---
        reconnect_count, reconnect_delay = 0, FIRST_RECONNECT_DELAY
        while reconnect_count < MAX_RECONNECT_COUNT:
            if FLAG_EXIT: # Check if another part of the code signaled an exit
                print "[MQTT Sender] Exiting _on_disconnect as FLAG_EXIT is True."
                return # Stop trying to reconnect if application is shutting down

            # It's good practice to also check network connectivity here before each attempt
            # to avoid unnecessary broker connection attempts when the network is truly down.
            if not check_network_connectivity():
                print "[MQTT Sender] Network is still offline. Delaying reconnect attempt..."
                # No need to increment reconnect_count if network is truly offline,
                # as it's not an MQTT broker issue. Just wait.
                time.sleep(reconnect_delay)
                # You might want to consider not incrementing reconnect_delay if network is offline
                # to avoid hitting MAX_RECONNECT_DELAY too quickly while just waiting for network.
                continue # Skip the reconnect attempt and go to next loop iteration

            print "[MQTT Sender] Reconnecting in {0} seconds (attempt {1}/{2})...".format(
                reconnect_delay, reconnect_count + 1, MAX_RECONNECT_COUNT
            )
            time.sleep(reconnect_delay) # Wait before the attempt

            try:
                # THIS IS THE EXPLICIT RECONNECT CALL!
                client.reconnect()
                print "[MQTT Sender] Reconnected successfully!"
                return # If reconnected, exit this reconnection loop
            except Exception, err:
                print "[MQTT Sender] Reconnect failed: {0}. Retrying...".format(err)

            # Increment attempt count and calculate next delay with exponential backoff
            reconnect_delay *= RECONNECT_RATE
            reconnect_delay = min(reconnect_delay, MAX_RECONNECT_DELAY) # Cap the delay
            reconnect_count += 1

        # If the loop finishes, it means MAX_RECONNECT_COUNT attempts failed
        print "[MQTT Sender] CRITICAL: Reconnect failed after {0} attempts within _on_disconnect. Signaling application to exit.".format(reconnect_count)
        FLAG_EXIT = True # Signal the main loop to terminate, triggering a daemon restart

    def connect(self):
        """
        Attempts to initiate an MQTT connection using connect_async.
        This is primarily for the *initial* connection attempt.
        The custom logic in _on_disconnect will handle subsequent reconnections.
        """
        if self.client.is_connected():
            return True # Already connected

        print "[MQTT Sender] Requesting MQTT connection via Paho client: {0}:{1}".format(self.broker_host, self.broker_port)
        try:
            # connect_async is non-blocking and works with loop_start()
            self.client.connect_async(self.broker_host, self.broker_port, keepalive=MQTT_KEEPALIVE)
            return True
        except Exception, e:
            # This catch is for errors *before* the connection attempt is even started by Paho (e.g., bad host string)
            # Most network errors during the actual connection will be handled by Paho's loop and reported via _on_disconnect
            print "[MQTT Sender] MQTT connection attempt error (from connect_async call): {0}".format(e)
            return False

    def publish_reading(self, reading):
        """Publishes a single sensor reading to MQTT."""
        if not self.client.is_connected():
            print "[MQTT Sender] MQTT client not connected. Cannot publish."
            return False

        topic = self.topic_base
        pubTime = int(time.time()) # Ensure pubTime is an integer timestamp

        # Prepare payload from the reading dictionary
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
            # Publish the message with QoS 1 for guaranteed delivery
            info = self.client.publish(topic, json.dumps(payload), qos=1)

            # Check the return code from the publish call itself
            if info.rc == mqtt.MQTT_ERR_SUCCESS:
                print "[MQTT Sender] Published to {0}: {1}".format(topic, payload)
                return True
            else:
                print "[MQTT Sender] Failed to publish (rc: {0}). Data will remain buffered.".format(info.rc)
                return False
        except Exception, e:
            print "[MQTT Sender] Error publishing to MQTT: {0}. Data will remain buffered.".format(e)
            return False

# --- Main Application Logic ---
def main():
    global FLAG_EXIT
    print "[MQTT Sender] Starting MQTT sender and SQLite reader..."

    # Initialize MqttHandler once at the start of the application.
    # Its internal loop_start() handles background connection management.
    mqtt_handler = MqttHandler(
        MQTT_BROKER_HOST, MQTT_BROKER_PORT, MQTT_CLIENT_ID, MQTT_TOPIC_BASE,
        MQTT_USERNAME, MQTT_PASSWORD
    )

    # Initiate the first connection attempt. Paho's loop will take over from here
    # for all subsequent reconnections as triggered by _on_disconnect.
    mqtt_handler.connect() 

    # Counter to track consecutive cycles spent waiting for MQTT connection.
    # This now acts as a secondary watchdog if _on_disconnect's logic fails persistently.
    connection_wait_cycles = 0 

    try: # Outer try block to encompass the while loop and ensure final cleanup
        while not FLAG_EXIT: # Loop until the FLAG_EXIT is set (e.g., by _on_disconnect)
            try: # Inner try block for exceptions within each loop iteration
                network_ok = check_network_connectivity()

                if network_ok:
                    if not mqtt_connected:
                        # If not connected, _on_disconnect should be actively trying to reconnect.
                        # We just increment a counter here to detect if this state persists for too long.
                        print "[MQTT Sender] Waiting for MQTT connection (Paho client status: {0})...".format(
                            "connected" if mqtt_handler.client.is_connected() else "disconnected"
                        )
                        connection_wait_cycles += 1

                        if connection_wait_cycles >= MAX_CONSECUTIVE_WAIT_CYCLES:
                            # If connection has been waiting for too many cycles,
                            # print a critical message and exit to allow system daemon to restart.
                            print "[MQTT Sender] CRITICAL: MQTT connection persistently failing after {0} cycles in main loop. Exiting to trigger system restart.".format(MAX_CONSECUTIVE_WAIT_CYCLES)
                            sys.exit(1) # Exit with non-zero status code
                    else: # mqtt_connected is True
                        # Reset the counter once connection is successful
                        connection_wait_cycles = 0 

                        # Only pick up and send data if MQTT is connected
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
                else: # Network is offline
                    print "[MQTT Sender] Network is offline. Not attempting MQTT connection."
                    # Reset the counter if the network itself is offline, as this is a different problem.
                    connection_wait_cycles = 0 

                time.sleep(SEND_INTERVAL_SECONDS) # Wait for the next cycle

            except KeyboardInterrupt:
                print "\n[MQTT Sender] KeyboardInterrupt detected. Shutting down gracefully..."
                FLAG_EXIT = True # Signal main loop to terminate
            except SystemExit: # This will catch the sys.exit(1) calls for planned exits
                print "\n[MQTT Sender] SystemExit signal received. Shutting down gracefully (likely from restart trigger)."
                FLAG_EXIT = True # Signal main loop to terminate
            except Exception, e:
                print "\n[MQTT Sender] An unexpected error occurred in main loop: {0}".format(e)
                # Reset counter on unexpected error, as the issue might be transient or different.
                connection_wait_cycles = 0 
                time.sleep(SEND_INTERVAL_SECONDS) # Wait before retrying

    finally: # This finally block ensures cleanup regardless of how the loop exits
        # --- Clean up resources ---
        print "[MQTT Sender] Initiating final shutdown sequence..."
        if mqtt_handler.client: # Ensure the client object exists
            try:
                # Disconnect if still connected
                if mqtt_handler.client.is_connected():
                    mqtt_handler.client.disconnect()
                    print "[MQTT Sender] MQTT client disconnected."
                # Always stop the background loop thread
                mqtt_handler.client.loop_stop() 
                print "[MQTT Sender] MQTT client loop stopped."
            except Exception, e:
                print "[MQTT Sender] Error during final MQTT client shutdown: {0}".format(e)

        print "[MQTT Sender] Application terminated."

if __name__ == "__main__":
    main()