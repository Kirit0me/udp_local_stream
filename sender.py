import json
import socket
import time
from datetime import datetime, timezone
import dateutil.parser  

# Configuration
UDP_IP = "127.0.0.1"
UDP_PORT = 5005
JSON_FILE = "radar_data.json"

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

print(f"Loading and sorting data for real-time replay...", flush=True)

try:
    with open(JSON_FILE, 'r') as f:
        raw_data = json.load(f)
        
        # 1. Parse timestamps and sort the list chronologically
        # We assume 'tola_utc' is the reference time for when the event happened.
        valid_data = []
        for track in raw_data:
            if 'tola_utc' in track:
                # Parse ISO string to datetime object
                # Handles "2025-09-02T18:13:49Z" format
                try:
                    dt = dateutil.parser.isoparse(track['tola_utc'])
                    # Ensure it has timezone info (UTC) for math
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    
                    track['_dt_obj'] = dt # Store temporary datetime object for sorting
                    valid_data.append(track)
                except ValueError:
                    print(f"Skipping track {track.get('track_id')} due to invalid date format.")

        # Sort by time
        valid_data.sort(key=lambda x: x['_dt_obj'])
        
        print(f"Loaded {len(valid_data)} valid tracks. Starting Replay...", flush=True)
        print("-" * 40)

        # 2. The Replay Loop
        start_time_wall_clock = time.time()
        
        if len(valid_data) > 0:
            first_packet_time = valid_data[0]['_dt_obj']
            
            for i, track in enumerate(valid_data):
                current_packet_time = track['_dt_obj']
                
                # Calculate how much time passed in the recorded data since the start
                time_offset_in_recording = (current_packet_time - first_packet_time).total_seconds()
                
                # Calculate how much time has passed in our real-world simulation
                time_passed_wall_clock = time.time() - start_time_wall_clock
                
                # The delay we need to wait is the difference
                sleep_duration = time_offset_in_recording - time_passed_wall_clock
                
                if sleep_duration > 0:
                    time.sleep(sleep_duration)
                
                # --- Send the Packet ---
                
                # Clean up the temp field before sending
                to_send = track.copy()
                del to_send['_dt_obj']
                
                # Add our system timestamp (ts_sent) for latency tracking
                to_send['ts_sent'] = datetime.utcnow().isoformat() + "Z"

                message = json.dumps(to_send).encode('utf-8')
                sock.sendto(message, (UDP_IP, UDP_PORT))
                
                print(f"[{datetime.now().strftime('%H:%M:%S:')}] Sent ID: {track.get('track_id')} (Recorded Delay: {time_offset_in_recording:.2f}s)")

        print("Replay finished.")

except FileNotFoundError:
    print(f"Error: '{JSON_FILE}' not found.")
except Exception as e:
    print(f"Error: {e}")