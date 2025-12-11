import json
import socket
import time
from datetime import datetime

# Configuration
UDP_IP = "127.0.0.1"
UDP_PORT = 5005
JSON_FILE = "radar_data.json" # Ensure this file exists in the same folder

# Setup Socket
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

print(f"Streaming data to {UDP_IP}:{UDP_PORT}...")

try:
    with open(JSON_FILE, 'r') as f:
        data = json.load(f)

        for track in data:
            # 1. Inject Sending Timestamp (UTC ISO Format)
            track['ts_sent'] = datetime.utcnow().isoformat() + "Z"

            # 2. Prepare and Send
            message = json.dumps(track).encode('utf-8')
            sock.sendto(message, (UDP_IP, UDP_PORT))
            
            print(f"Sent ID: {track.get('track_id')} | Time: {track['ts_sent']}")
            
            # 3. Simulate real-time gap (0.5 seconds)
            time.sleep(0.5) 
            
except FileNotFoundError:
    print(f"Error: '{JSON_FILE}' not found. Please create it with your data.")
except Exception as e:
    print(f"Error: {e}")