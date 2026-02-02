import happybase
import json
import os
import glob

# 1. Connect to HBase (Ensure container is running and port 9090 is open)
try:
    connection = happybase.Connection('localhost', port=9090)
    connection.open()
    table = connection.table('sessions')
    print("Connected to HBase successfully!")
except Exception as e:
    print(f"Connection failed: {e}")
    exit()

# 2. Path to your 20 session files
project_path = r'C:\Users\nicolas.shyaka\Documents\Personal\AUCA\Big Data Analytics Final Project'
json_files = glob.glob(os.path.join(project_path, 'sessions_*.json'))

# 3. Process each file
for file_path in json_files:
    print(f"Processing {os.path.basename(file_path)}...")
    with open(file_path, 'r') as f:
        try:
            sessions_data = json.load(f)
            
            # Ensure data is a list (if your JSON is an array)
            if not isinstance(sessions_data, list):
                sessions_data = [sessions_data]
            
            with table.batch() as b:
                for session in sessions_data:
                    # Use session_id as the Row Key
                    row_key = str(session.get('session_id', os.urandom(8).hex()))
                    
                    # Map JSON fields to your Column Families
                    # Note: HBase requires bytes/strings
                    payload = {
                        b'meta:user_id': str(session.get('user_id', '')).encode(),
                        b'meta:timestamp': str(session.get('timestamp', '')).encode(),
                        b'geo:city': str(session.get('city', '')).encode(),
                        b'geo:country': str(session.get('country', '')).encode(),
                        b'device:type': str(session.get('device_type', '')).encode(),
                        b'stats:duration': str(session.get('duration', '0')).encode(),
                        b'events:log': json.dumps(session.get('events', [])).encode()
                    }
                    b.put(row_key, payload)
        except Exception as e:
            print(f"Error skipping {file_path}: {e}")

connection.close()
print("Finished loading all sessions.")
