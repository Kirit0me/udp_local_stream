import json
import random
import heapq
from datetime import datetime, timedelta
from faker import Faker
from geopy.distance import distance as geodesic_dist
from geopy.point import Point

fake = Faker()

# --- CONFIGURATION ---
TOTAL_SAMPLES_PER_TYPE = 1_000_000 # 3 Million Total
START_TIME = datetime.utcnow()
OUTPUT_FILE = "authentic_big_data.json"

# --- HELPER FUNCTIONS ---
def calculate_nmea_checksum(sentence):
    calc_cksum = 0
    for s in sentence:
        calc_cksum ^= ord(s)
    return hex(calc_cksum)[2:].upper().zfill(2)

# --- ENTITY CLASSES ---

class Entity:
    def __init__(self, _id, lat, lon, speed, heading):
        self.id = _id
        self.lat = lat
        self.lon = lon
        self.speed = speed # Base speed unit depends on type
        self.heading = heading
        self.next_ping_offset = random.uniform(0, 1000) # ms from start

    def update_physics(self, time_delta_sec):
        # Physics: Move point based on speed and heading
        # 1 knot approx 0.000514444 km/s; 1 kph = 0.000277778 km/s
        # Simplified: We treat speed as "units per hour" roughly for simulation
        km = (self.speed * 0.0005) * time_delta_sec 
        start = Point(self.lat, self.lon)
        dest = geodesic_dist(kilometers=km).destination(start, self.heading)
        self.lat = dest.latitude
        self.lon = dest.longitude
        self.heading = (self.heading + random.uniform(-1, 1)) % 360

class Ship(Entity):
    def __init__(self):
        mmsi = fake.numerify(text='2######00')
        super().__init__(mmsi, random.uniform(20.0, 22.0), random.uniform(70.0, 72.0), 
                         speed=random.uniform(10, 20), heading=random.uniform(0, 360))
        self.name = fake.company().upper()
        self.callsign = fake.bothify(text='????').upper()
        self.interval = 5000 # 5s

    def generate(self, dt_str):
        # Simulate !AIVDM
        raw = f"!AIVDM,1,1,,A,{fake.bothify(text='13sIek001t52???;imP`ro8<0000')},0*26"
        return {
            "source_type": "AIS",
            "MMSI": self.id,
            "TIMESTAMP": dt_str,
            "LATITUDE": round(self.lat, 6),
            "LONGITUDE": round(self.lon, 6),
            "SPEED": round(self.speed, 1),
            "COURSE": round(self.heading, 1),
            "HEADING": int(self.heading),
            "NAME": self.name,
            "CALLSIGN": self.callsign,
            "RAW_MSG": raw
        }

class Plane(Entity):
    def __init__(self):
        icao = fake.hexify(text='^^^^^^')
        super().__init__(icao, random.uniform(18.0, 24.0), random.uniform(68.0, 74.0), 
                         speed=random.uniform(400, 550), heading=random.uniform(0, 360))
        self.callsign = fake.bothify(text='AX###').upper()
        self.altitude = 35000
        self.interval = 500 # 0.5s

    def generate(self, dt_str):
        # Simulate Mode-S Hex
        raw = f"*8D{self.id}9944{fake.numerify(text='######')};"
        return {
            "source_type": "ADSB",
            "ICAO": self.id,
            "TIMESTAMP": dt_str,
            "LATITUDE": round(self.lat, 6),
            "LONGITUDE": round(self.lon, 6),
            "ALTITUDE_FT": self.altitude,
            "SPEED_KTS": round(self.speed, 1),
            "HEADING": round(self.heading, 1),
            "CALLSIGN": self.callsign,
            "RAW_MSG": raw
        }

class Car(Entity):
    def __init__(self):
        vid = fake.bothify(text='GPS-##')
        super().__init__(vid, random.uniform(21.0, 21.2), random.uniform(72.5, 72.7), 
                         speed=random.uniform(30, 100), heading=random.uniform(0, 360))
        self.interval = 1000 # 1s

    def generate(self, dt_str):
        # Simulate $GPRMC
        base = f"GPRMC,123456,A,{self.lat:.4f},N,{self.lon:.4f},E,{self.speed/1.8:.1f},{self.heading:.1f},,,"
        raw = f"${base}*{calculate_nmea_checksum(base)}"
        return {
            "source_type": "GPS",
            "VEHICLE_ID": self.id,
            "TIMESTAMP": dt_str,
            "LATITUDE": round(self.lat, 6),
            "LONGITUDE": round(self.lon, 6),
            "SPEED_KPH": round(self.speed, 1),
            "HEADING": round(self.heading, 1),
            "RAW_MSG": raw
        }

# --- MAIN GENERATOR ---

fleet = []
# Create a recycled fleet (100 of each type) to generate millions of points
fleet.extend([Ship() for _ in range(100)])
fleet.extend([Plane() for _ in range(100)])
fleet.extend([Car() for _ in range(100)])

# Priority Queue (Min-Heap) for chronological event ordering
# Item: (time_ms, fleet_index)
event_queue = []
for i, entity in enumerate(fleet):
    heapq.heappush(event_queue, (entity.next_ping_offset, i))

counts = {"AIS": 0, "ADSB": 0, "GPS": 0}
total_generated = 0
target_total = TOTAL_SAMPLES_PER_TYPE * 3

big_data_list = [] # <--- The massive array

print(f"Generating {target_total} packets in RAM...")

try:
    while total_generated < target_total:
        # 1. Get next chronological event
        sim_time_ms, idx = heapq.heappop(event_queue)
        entity = fleet[idx]
        
        # Check quota
        type_key = "AIS" if isinstance(entity, Ship) else "ADSB" if isinstance(entity, Plane) else "GPS"
        if counts[type_key] >= TOTAL_SAMPLES_PER_TYPE:
            if all(c >= TOTAL_SAMPLES_PER_TYPE for c in counts.values()):
                break
            continue

        # 2. Physics & Generate
        entity.update_physics(entity.interval / 1000.0)
        
        event_dt = START_TIME + timedelta(milliseconds=sim_time_ms)
        dt_str = event_dt.isoformat(timespec='milliseconds') + "Z"
        
        packet = entity.generate(dt_str)
        big_data_list.append(packet)
        
        # 3. Update State
        counts[type_key] += 1
        total_generated += 1
        
        # 4. Schedule Next
        jitter = random.uniform(-0.1 * entity.interval, 0.1 * entity.interval)
        next_ping = sim_time_ms + entity.interval + jitter
        heapq.heappush(event_queue, (next_ping, idx))
        
        if total_generated % 100000 == 0:
            print(f"Generated {total_generated} / {target_total}...", flush=True)

    print("Writing to JSON file... (This may take a moment)")
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(big_data_list, f, indent=2)
    print("Done!")

except KeyboardInterrupt:
    print("Stopped early. Saving what we have...")
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(big_data_list, f, indent=2)