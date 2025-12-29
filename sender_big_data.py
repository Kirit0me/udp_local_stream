import json
import socket
import time
from datetime import datetime
import dateutil.parser # pip install python-dateutil

# Configuration
UDP_IP = "127.0.0.1"
UDP_PORT = 5005
JSON_FILE = "authentic_big_data.json"

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

print(f"Loading {JSON_FILE} into RAM...", flush=True)

try:
    with open(JSON_FILE, 'r') as f:
        data = json.load(f)
        
    if not data:
        print("File is empty")
        exit()
        
    print(f"Loaded {len(data)} records. Starting Stream...")
    
    # Synchronization Anchor
    t0_recorded = dateutil.parser.isoparse(data[0]['TIMESTAMP'])
    t0_wallclock = time.time()
    
    count = 0
    
    for packet in data:
        # 1. Timing Logic
        target_time = dateutil.parser.isoparse(packet['TIMESTAMP'])
        offset = (target_time - t0_recorded).total_seconds()
        
        target_wallclock = t0_wallclock + offset
        current_wallclock = time.time()
        
        sleep_dur = target_wallclock - current_wallclock
        if sleep_dur > 0:
            time.sleep(sleep_dur)
            
        # 2. Add Telemetry Timestamp
        packet['ts_sent'] = datetime.utcnow().isoformat() + "Z"
        
        # 3. Send
        msg = json.dumps(packet).encode('utf-8')
        sock.sendto(msg, (UDP_IP, UDP_PORT))
        
        count += 1
        if count % 1000 == 0:
            # Determine which ID to show
            uid = packet.get('MMSI') or packet.get('ICAO') or packet.get('VEHICLE_ID')
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Sent {count}. Last ID: {uid}")

except FileNotFoundError:
    print(f"Error: {JSON_FILE} not found.")
except KeyboardInterrupt:
    print("\nStream stopped.")