const dgram = require('dgram');
const mongoose = require('mongoose');
const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const cors = require('cors');

// --- 1. MongoDB Setup ---
mongoose.connect('mongodb://127.0.0.1:27017/radarDB')
.then(() => console.log("MongoDB Connected"))
.catch(err => console.error("MongoDB Connection Error:", err));

const trackSchema = new mongoose.Schema({
    track_id: String,
    freq_mhz: Number,
    prf_hz: Number,
    pw_us: Number,
    amplitude_db: Number,
    scan_type: String,
    tofa_utc: Date,
    own_position: {
        latitude_degdec: Number,
        longitude_degdec: Number
    },
    // Timestamps for Tracking
    ts_sent: Date,      // From Python
    ts_received: Date,  // From Node UDP
    ts_stored: Date     // From Node Mongo
}, { strict: false });

const Track = mongoose.model('Track', trackSchema);

// --- 2. Express + Socket.io Setup ---
const app = express();
app.use(cors());
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*" } 
});

// REST Endpoint to fetch recent history
app.get('/tracks', async (req, res) => {
    const tracks = await Track.find().sort({_id: -1}).limit(50);
    res.json(tracks);
});

// --- 3. UDP Receiver Setup ---
const udpSocket = dgram.createSocket('udp4');

udpSocket.on('message', async (msg, rinfo) => {
    // A. Capture Receive Time IMMEDIATELY
    const receivedTime = new Date();

    try {
        const jsonString = msg.toString();
        const data = JSON.parse(jsonString);

        // B. Add receive time to data object
        data.ts_received = receivedTime;
        
        // Remove incoming _id to prevent Mongo collision
        delete data._id; 
        
        const newTrack = new Track(data);
        
        // C. Save to DB
        await newTrack.save();

        // D. Capture Stored Time
        const storedTime = new Date();
        data.ts_stored = storedTime;

        // Log for debugging
        const networkDelay = receivedTime - new Date(data.ts_sent);
        console.log(`Received ID: ${data.track_id} | Net Delay: ${networkDelay}ms`);

        // E. Emit to Frontend
        io.emit('new_track', data);

    } catch (err) {
        console.error("Error processing message:", err);
    }
});

udpSocket.bind(5005);

server.listen(3000, () => {
    console.log('Backend Server running on http://localhost:3000');
});