# UDP Stream

A full-stack simulation and visualization pipeline for real-time radar tracking data. This system mimics a hardware sensor streaming UDP packets, captures them in a backend, stores them in MongoDB, and visualizes them on a live dashboard with performance metrics.


# Installation

0. MongoDB service should be running.

1. Setup Backend

`cd server`

`npm install`

`node index.js`

Also need python virtual environment

`python3 -m venv venv`

`source venv/bin/activate`

`pip install python-dateutil`

2. Setup frontend

You can open the `frontend/index.html` in your browser, or use VSC's Live server to activate. 

3. Start streaming data from json

`python sender.py`



# For big data and FastAPI : 

1. Python utilities using venv: 

`pip install fastapi uvicorn motor websockets faker geopy python-dateutil`

2. Starting new database: 

`mongod`

OR (If you have permission errors, run locally)

`mkdir -p mongodb_data`

`mongod --dbpath ./mongodb_data`

3. Generating Data (All in venv itself)

`python3 generator_new.py`

4. Starting backend server

`cd server`

`uvicorn main:app --reload --port 8000`

5. Then send through `sender_big_data.py` and check in `index_big_data.html`

6. to drop db : `mongosh authenticDB --eval "db.dropDatabase()"`