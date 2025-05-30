import sqlite3
import time # For converting timestamp back to readable format

# Data Buffering Settings (must match the other script)
DB_FILE = '/root/work/ups/db/sensor_data.db'

def read_all_sensor_data():
    """Reads and prints all data from the sensor_readings table."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # Select all data from the table
        cursor.execute('SELECT timestamp, acVoltage, acStatus, upsVoltage, upsStatus FROM sensor_readings ORDER BY timestamp ASC')
        
        rows = cursor.fetchall()

        if not rows:
            print "No data found in the database."
            return

        print "--- Sensor Data from Database ---"
        print "{:<20} {:<12} {:<10} {:<12} {:<10}".format(
            "Timestamp", "AC Voltage", "AC Status", "UPS Voltage", "UPS Status"
        )
        print "-" * 65 # Separator

        for row in rows:
            timestamp_unix = row[0]
            ac_voltage = row[1]
            ac_status = row[2]
            ups_voltage = row[3]
            ups_status = row[4]

            # Convert Unix timestamp to a human-readable format
            # Using time.ctime() for simplicity in Python 2
            readable_timestamp = time.ctime(timestamp_unix) 

            print "{:<20} {:<12.2f} {:<10d} {:<12.2f} {:<10d}".format(
                readable_timestamp, ac_voltage, ac_status, ups_voltage, ups_status
            )

        print "---------------------------------"

    except sqlite3.Error, e:
        print "Error reading from database: {0}".format(e)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    read_all_sensor_data()

