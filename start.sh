#!/bin/bash

pkill -f "web_server.py"

/usr/bin/python3 web_server.py  --port 5000 --peer 3 &
/usr/bin/python3 web_server.py  --port 5001 --peer 3 &
/usr/bin/python3 web_server.py  --port 5002 --peer 3 &
