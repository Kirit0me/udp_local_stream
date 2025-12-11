# UDP Stream

A full-stack simulation and visualization pipeline for real-time radar tracking data. This system mimics a hardware sensor streaming UDP packets, captures them in a backend, stores them in MongoDB, and visualizes them on a live dashboard with performance metrics.


# Installation

0. MongoDB service should be running.

1. Setup Backend

`cd server`

`npm install`

`node index.js`

2. Setup frontend

You can open the `frontend/index.html` in your browser, or use VSC's Live server to activate. 

3. Start streaming data from json

`python sender.py`
