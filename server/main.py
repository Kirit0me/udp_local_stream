import asyncio
import json
from datetime import datetime
from typing import List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient

# Config
UDP_IP = "127.0.0.1"
UDP_PORT = 5005
MONGO_URL = "mongodb://localhost:27017"
DB_NAME = "authenticDB"
COLLECTION_NAME = "stream_data"
BATCH_INTERVAL = 0.1 # 100ms batches

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Global Buffer
packet_buffer = []
total_processed_count = 0

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        # Send recent history (Last 1000)
        if collection is not None:
            cursor = collection.find().sort("ts_stored", -1).limit(1000)
            history = await cursor.to_list(length=1000)
            if history:
                # Send as batch
                msg = {
                    "type": "batch", 
                    "data": [fix_oid(d) for d in reversed(history)],
                    "total_count": total_processed_count
                }
                await websocket.send_text(json.dumps(msg))

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        if not self.active_connections: return
        json_str = json.dumps(message)
        for connection in list(self.active_connections):
            try:
                await connection.send_text(json_str)
            except:
                self.disconnect(connection)

manager = ConnectionManager()
db_client = None
collection = None

def fix_oid(doc):
    if "_id" in doc: doc["_id"] = str(doc["_id"])
    return doc

class UDPProtocol(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        print(f"UDP Listener on {UDP_IP}:{UDP_PORT}")

    def datagram_received(self, data, addr):
        asyncio.create_task(handle_packet(data))

async def handle_packet(data):
    global total_processed_count
    try:
        msg = json.loads(data.decode())
        # 1. Received Time
        msg["ts_received"] = datetime.utcnow().isoformat() + "Z"
        packet_buffer.append(msg)
        total_processed_count += 1
    except:
        pass

async def batch_processor():
    global packet_buffer
    print("Batch Processor Started")
    while True:
        await asyncio.sleep(BATCH_INTERVAL)
        if not packet_buffer: continue
            
        current_batch = packet_buffer[:]
        packet_buffer.clear()
        
        # 2. Add Stored Timestamp (CRITICAL for calculating DB Latency)
        now_str = datetime.utcnow().isoformat() + "Z"
        for p in current_batch:
            p["ts_stored"] = now_str
            if "_id" in p: del p["_id"]
            
        if collection is not None:
            await collection.insert_many(current_batch)
            
        await manager.broadcast({
            "type": "batch",
            "data": [fix_oid(p) for p in current_batch],
            "total_count": total_processed_count
        })

@app.on_event("startup")
async def startup():
    global db_client, collection, total_processed_count
    db_client = AsyncIOMotorClient(MONGO_URL)
    collection = db_client[DB_NAME][COLLECTION_NAME]
    total_processed_count = await collection.count_documents({})
    
    loop = asyncio.get_running_loop()
    await loop.create_datagram_endpoint(lambda: UDPProtocol(), local_addr=(UDP_IP, UDP_PORT))
    asyncio.create_task(batch_processor())

@app.on_event("shutdown")
async def shutdown():
    db_client.close()

@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True: await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)