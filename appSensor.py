import json
import minimalmodbus
import serial
import sqlite3
import threading
import time
from minimalmodbus import * # Importing all global variables from minimalmodbus


# Data Buffering Settings
DB_FILE = '/root/work/ups/db/sensor_data.db'
READ_INTERVAL_SECONDS = 600 # How often to read sensors


# --- Global Flags and Locks ---
db_lock = threading.Lock() # To prevent race conditions when accessing the database

# --- Database Management ---
def setup_database():
    """Initializes the SQLite database and table."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Original CREATE TABLE statement (no WAL pragma)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                acVoltage INTEGER NOT NULL,
                acStatus INTEGER NOT NULL,
                upsVoltage INTEGER NOT NULL,
                upsStatus INTEGER NOT NULL
            )
        ''')
        conn.commit()
        # Python 2 print statement with .format()
        print "[Modbus Reader] Database '{0}' initialized successfully.".format(DB_FILE)
    except sqlite3.Error, e: # Python 2 exception syntax
        print "[Modbus Reader] Error setting up database: {0}".format(e)
    finally:
        if conn:
            conn.close()

def save_reading_to_db(timestamp, acVoltage, acStatus, upsVoltage, upsStatus):
    """Saves a sensor reading to the SQLite database."""
    with db_lock:
        conn = None
        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO sensor_readings (timestamp, acVoltage, acStatus, upsVoltage, upsStatus)
                VALUES (?, ?, ?, ?, ?)
            ''', (timestamp, acVoltage, acStatus, upsVoltage, upsStatus))
            conn.commit()
            # Python 2 print statement
            print "[Modbus Reader] Saved to DB: {0}, AC={1}/{2}, UPS={3}/{4}".format(
                timestamp, acVoltage, acStatus, upsVoltage, upsStatus)
        except sqlite3.Error, e: # Python 2 exception syntax
            print "[Modbus Reader] Error saving to database: {0}".format(e) # Removed retry logic
        finally:
            if conn:
                conn.close()

# --- Modbus Instrument Setup ---
upsId = 1
acId = 1

instrument_ups = None
instrument_ac = None

try:
    instrument_ups = minimalmodbus.Instrument('/dev/ttyS1', upsId)
    instrument_ups.serial.baudrate = 9600
    instrument_ups.serial.bytesize = 8
    instrument_ups.serial.parity = serial.PARITY_NONE
    instrument_ups.serial.stopbits = 1
    instrument_ups.serial.timeout = 4
    instrument_ups.clear_buffers_before_each_transaction = True
    print "[Modbus Reader] UPS Instrument initialized."
    # Initial dummy read to ensure connection is live
    instrument_ups.read_register(4, 0, 3, False)
    print "[Modbus Reader] Initial UPS Modbus read successful."

    instrument_ac = minimalmodbus.Instrument('/dev/ttyS2', acId)
    instrument_ac.serial.baudrate = 9600
    instrument_ac.serial.bytesize = 8
    instrument_ac.serial.parity = serial.PARITY_NONE
    instrument_ac.serial.stopbits = 1
    instrument_ac.serial.timeout = 0.1 # Shorter timeout for AC, consider if appropriate
    instrument_ac.clear_buffers_before_each_transaction = True
    print "[Modbus Reader] AC Instrument initialized."
    # Initial dummy read to ensure connection is live
    instrument_ac.read_register(4, 0, 3, False)
    print "[Modbus Reader] Initial AC Modbus read successful."

except minimalmodbus.ModbusException, e:
    print "[Modbus Reader] Modbus Initialization Error: {0}".format(e)
except serial.SerialException, e:
    print "[Modbus Reader] Serial Port Initialization Error: {0}".format(e)
except Exception, e:
    print "[Modbus Reader] Unexpected Initialization Error: {0}".format(e)

# --- Sensor Reading Functions ---
def readAll():
    data = []
    acVoltage = 0
    acStatus = 1 # 0: OK, 1: Error/Low

    upsVoltage = 0
    upsStatus = 1 # 0: OK, 1: Error/Low

    # Read AC Voltage
    try:
        if instrument_ac: # Ensure instrument is initialized
            acVoltage = int(instrument_ac.read_register(0x48, 2, 3, False)) # Ensure float conversion
            if acVoltage >= 90:
                acStatus = 0
            else:
                acStatus = 1
        else:
            print "[Modbus Reader] AC Instrument not available, skipping AC read."
    except minimalmodbus.ModbusException, e:
        print "Error in reading AC voltage (ModbusException): {0}".format(e)
        if instrument_ac and instrument_ac.serial.isOpen():
            instrument_ac.serial.close()
            print "[Modbus Reader] AC serial port closed, attempting reconnect..."
            time.sleep(1) 
            try:
                instrument_ac.serial.open()
                print "[Modbus Reader] AC serial port re-opened."
            except serial.SerialException, serr:
                print "[Modbus Reader] Failed to re-open AC serial port: {0}".format(serr)
    except serial.SerialException, e:
        print "Error in reading AC voltage (SerialException): {0}".format(e)
    except Exception, e:
        print "Unexpected error in reading AC voltage: {0}".format(e)
        
    # Read UPS Voltage
    try:
        if instrument_ups: # Ensure instrument is initialized
            upsVoltage = int(instrument_ups.read_register(0x48, 2, 3, False)) # Ensure float conversion
            if upsVoltage >= 90:
                upsStatus = 0
            else:
                upsStatus = 1
        else:
            print "[Modbus Reader] UPS Instrument not available, skipping UPS read."
    except minimalmodbus.ModbusException, e:
        print "Error in reading UPS voltage (ModbusException): {0}".format(e)
        if instrument_ups and instrument_ups.serial.isOpen():
            instrument_ups.serial.close()
            print "[Modbus Reader] UPS serial port closed, attempting reconnect..."
            time.sleep(1) 
            try:
                instrument_ups.serial.open()
                print "[Modbus Reader] UPS serial port re-opened."
            except serial.SerialException, serr:
                print "[Modbus Reader] Failed to re-open UPS serial port: {0}".format(serr)
    except serial.SerialException, e:
        print "Error in reading UPS voltage (SerialException): {0}".format(e)
    except Exception, e:
        print "Unexpected error in reading UPS voltage: {0}".format(e)

    data.append({
        'timestamp':int(time.time()), # Cast to int for INTEGER column
        'acVoltage':acVoltage,
        'acStatus':acStatus,
        'upsVoltage': upsVoltage,
        'upsStatus':upsStatus
        })

    return data
    

# --- Main Application Logic ---
def main():
    print "[Modbus Reader] Starting Modbus sensor reader and SQLite writer..."
    setup_database()

    while True:
        sensor_data = readAll()

        if sensor_data:
            # sensor_data is a list with one dictionary as per your readAll() implementation
            for data_entry in sensor_data: 
                save_reading_to_db(
                    data_entry['timestamp'], data_entry['acVoltage'], data_entry['acStatus'],
                    data_entry['upsVoltage'], data_entry['upsStatus']
                )
        else:
            print "[Modbus Reader] No sensor data read or Modbus error."

        time.sleep(READ_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
