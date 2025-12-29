import json
import random
from datetime import datetime, timedelta
from faker import Faker
from geopy.distance import distance as geodesic_dist
from geopy.point import Point
from pyais.encode import encode_dict

fake = Faker()

# Configuration
DURATION_SECONDS = 60       
START_TIME = datetime.utcnow()
OUTPUT_FILE = "authentic_data.json"

# --- HELPER: GPS Checksum Calculator ---
def calculate_nmea_checksum(sentence):
    """Calculates standard NMEA XOR checksum for GPS/AIS"""
    calc_cksum = 0
    for s in sentence:
        calc_cksum ^= ord(s)
    return hex(calc_cksum)[2:].upper().zfill(2)

# --- CLASSES FOR DIFFERENT DATA TYPES ---

class MovingEntity:
    def __init__(self, _id, lat, lon, speed, heading, alt=0):
        self.id = _id
        self.lat = lat
        self.lon = lon
        self.speed = speed
        self.heading = heading
        self.altitude = alt
        self.next_ping = random.uniform(0, 500) # Start randomly

    def move(self, duration_sec):
        # Physics: Distance = Speed * Time
        # Speed in knots -> km/s
        km_traveled = (self.speed * 0.000514444) * duration_sec
        
        start_point = Point(self.lat, self.lon)
        dest = geodesic_dist(kilometers=km_traveled).destination(start_point, self.heading)
        
        self.lat = dest.latitude
        self.lon = dest.longitude
        
        # Jitter heading slightly (brownian motion)
        self.heading = (self.heading + random.uniform(-1, 1)) % 360

class Ship(MovingEntity):
    def __init__(self, mmsi, type_code):
        super().__init__(mmsi, random.uniform(20.0, 22.0), random.uniform(70.0, 72.0), 
                         random.uniform(10, 20), random.uniform(0, 360))
        self.mmsi = mmsi
        self.type = type_code
        self.name = fake.company().upper()
        self.callsign = fake.bothify(text='????')
        self.interval = 2000 # AIS sends every ~2s
        
    def generate_packet(self, timestamp):
        # 1. Use pyais to generate REAL !AIVDM string
        nmea_payload = {
            'type': 1, 'repeat': 0, 'mmsi': str(self.mmsi),
            'status': 0, 'turn': 0, 'speed': round(self.speed, 1),
            'accuracy': False, 'lon': self.lon, 'lat': self.lat,
            'course': self.heading, 'heading': int(self.heading),
            'second': timestamp.second, 'maneuver': 0,
        }
        try:
            raw_list = encode_dict(nmea_payload, radio_channel="A", talker_id="AIVDM")
            raw_msg = raw_list[0] if raw_list else ""
        except:
            raw_msg = "ERROR_ENCODING"

        # 2. Return JSON matching VT Explorer style
        return {
            "source_type": "AIS",
            "MMSI": self.mmsi,
            "TIMESTAMP": timestamp.isoformat(timespec='milliseconds') + "Z",
            "LATITUDE": round(self.lat, 6),
            "LONGITUDE": round(self.lon, 6),
            "SPEED": round(self.speed, 1),
            "COURSE": round(self.heading, 1),
            "HEADING": int(self.heading),
            "NAME": self.name,
            "CALLSIGN": self.callsign,
            "RAW_MSG": raw_msg # !AIVDM...
        }

class Plane(MovingEntity):
    def __init__(self, icao):
        super().__init__(icao, random.uniform(18.0, 25.0), random.uniform(68.0, 75.0), 
                         random.uniform(400, 550), random.uniform(0, 360), alt=35000)
        self.icao = icao
        self.callsign = fake.bothify(text='AX###').upper()
        self.interval = 500 # ADSB sends every ~0.5s

    def generate_packet(self, timestamp):
        # 1. Simulate Raw ADS-B Hex (AVR Format)
        # Real calculation requires bit-packing, here we simulate the structure:
        # *[ICAO][DATA][CRC];
        dummy_hex = f"8D{self.icao}9944{random.randint(100000,999999)}"
        
        return {
            "source_type": "ADSB",
            "ICAO": self.icao,
            "TIMESTAMP": timestamp.isoformat(timespec='milliseconds') + "Z",
            "LATITUDE": round(self.lat, 6),
            "LONGITUDE": round(self.lon, 6),
            "ALTITUDE_FT": int(self.altitude),
            "SPEED_KTS": round(self.speed, 1),
            "HEADING": round(self.heading, 1),
            "CALLSIGN": self.callsign,
            "RAW_MSG": f"*{dummy_hex};" # Standard AVR raw format
        }

class Car(MovingEntity):
    def __init__(self, vehicle_id):
        super().__init__(vehicle_id, random.uniform(21.0, 21.1), random.uniform(72.5, 72.6), 
                         random.uniform(20, 60), random.uniform(0, 360), alt=50)
        self.vehicle_id = vehicle_id
        self.interval = 1000 # GPS sends every 1s

    def generate_packet(self, timestamp):
        # 1. Generate standard NMEA $GPRMC string
        # Format: $GPRMC,hhmmss.ss,A,lat,N,lon,E,spd,cog,ddmmyy,,*cs
        
        ts_str = timestamp.strftime("%H%M%S.%f")[:9] # hhmmss.ss
        date_str = timestamp.strftime("%d%m%y")
        
        # Convert Dec degree to NMEA degree (ddmm.mmmm)
        def to_nmea_deg(deg):
            d = int(deg)
            m = (deg - d) * 60
            return f"{d * 100 + m:09.4f}"

        lat_nmea = to_nmea_deg(abs(self.lat))
        lon_nmea = to_nmea_deg(abs(self.lon))
        lat_dir = 'N' if self.lat >= 0 else 'S'
        lon_dir = 'E' if self.lon >= 0 else 'W'
        
        base_msg = f"GPRMC,{ts_str},A,{lat_nmea},{lat_dir},{lon_nmea},{lon_dir},{self.speed:.1f},{self.heading:.1f},{date_str},,"
        checksum = calculate_nmea_checksum(base_msg)
        raw_nmea = f"${base_msg}*{checksum}"

        return {
            "source_type": "GPS",
            "VEHICLE_ID": self.vehicle_id,
            "TIMESTAMP": timestamp.isoformat(timespec='milliseconds') + "Z",
            "LATITUDE": round(self.lat, 6),
            "LONGITUDE": round(self.lon, 6),
            "SPEED_KPH": round(self.speed * 1.852, 1), # Knots to KPH
            "HEADING": round(self.heading, 1),
            "RAW_MSG": raw_nmea # $GPRMC...
        }

# --- MAIN SIMULATION LOOP ---

entities = []
# Create Fleet
entities.extend([Ship(fake.numerify(text='2######00'), 70) for _ in range(3)]) # 3 Ships
entities.extend([Plane(fake.hexify(text='^^^^^^')) for _ in range(3)])        # 3 Planes
entities.extend([Car(fake.bothify(text='GPS-##')) for _ in range(2)])         # 2 Cars

print(f"Simulating {len(entities)} mixed entities...")

data_log = []
sim_cursor_ms = 0.0
max_time_ms = DURATION_SECONDS * 1000.0
step_size_ms = 10 # Physics fidelity

while sim_cursor_ms < max_time_ms:
    for entity in entities:
        if sim_cursor_ms >= entity.next_ping:
            
            # Physics Move
            jitter = random.uniform(-1, 1) # Tiny timing error
            actual_time = entity.next_ping + jitter
            event_dt = START_TIME + timedelta(milliseconds=actual_time)
            
            # Move entity forward by the time since its last ping
            # (Approximation: we just move it by its interval duration)
            entity.move(entity.interval / 1000.0)
            
            # Generate Data
            packet = entity.generate_packet(event_dt)
            data_log.append(packet)
            
            # Schedule next
            # Add realistic jitter (e.g. +/- 10% of interval)
            jitter_ms = random.uniform(-entity.interval * 0.1, entity.interval * 0.1)
            entity.next_ping += (entity.interval + jitter_ms)

    sim_cursor_ms += step_size_ms

# Sort strictly by timestamp for the Sender
data_log.sort(key=lambda x: x['TIMESTAMP'])

with open(OUTPUT_FILE, 'w') as f:
    json.dump(data_log, f, indent=2)

print(f"Generated {len(data_log)} packets. Sample Raw Data:")
print(f"AIS:  {data_log[0]['RAW_MSG'] if data_log else ''}")