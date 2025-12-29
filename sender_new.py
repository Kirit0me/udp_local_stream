import json
import socket
import time
from datetime import datetime, timezone
import dateutil.parser
import sys

# Configuration
UDP_IP = "127.0.0.1"
UDP_PORT = 5005
JSON_FILE = "authentic_data.json"

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

print(f"Initializing High-Precision Replay...", flush=True)

try:
    with open(JSON_FILE, 'r') as f:
        data = json.load(f)
        
    # 1. Pre-process timestamps
    # We convert strings to datetime objects ONCE before the loop starts
    # to avoid processing lag during the actual replay.
    playlist = []
    for track in data:
        if 'tola_utc' in track:
            dt = dateutil.parser.isoparse(track['tola_utc'])
            if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
            track['_dt'] = dt
            playlist.append(track)
            
    # Sort just in case
    playlist.sort(key=lambda x: x['_dt'])
    
    print(f"Loaded {len(playlist)} packets. Starting Stream...", flush=True)
    
    if not playlist:
        print("No data found.")
        sys.exit()

    # 2. Synchronization
    # We map the First Packet's Recorded Time to "NOW"
    t0_recorded = playlist[0]['_dt']
    t0_wallclock = time.time()

    for i, track in enumerate(playlist):
        target_time = track['_dt']
        
        # Calculate exactly when this packet should be sent relative to start
        # This preserves the exact millisecond gap from the JSON
        offset_seconds = (target_time - t0_recorded).total_seconds()
        
        # Calculate the actual target time in our physical world
        target_wallclock = t0_wallclock + offset_seconds
        
        # Determine how long to sleep
        current_wallclock = time.time()
        sleep_duration = target_wallclock - current_wallclock
        
        if sleep_duration > 0:
            time.sleep(sleep_duration)
            
        # Clean up temp field
        to_send = track.copy()
        del to_send['_dt']
        
        # Add Sender Timestamp for latency checks
        to_send['ts_sent'] = datetime.utcnow().isoformat() + "Z"

        # Send
        msg = json.dumps(to_send).encode('utf-8')
        sock.sendto(msg, (UDP_IP, UDP_PORT))
        
        # Log with Millisecond Precision
        # This proves we are sending at specific millisecond intervals
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Sent {track.get('type')} ID:{track.get('track_id')}")

except Exception as e:
    print(f"Error: {e}")