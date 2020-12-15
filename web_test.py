import json
import time
import requests
from starter_code.withoutksp import allocate_flight_computers, commandline_args, readout_state

from utils import call_peer

r = None
timestep = 0

flight_computers = allocate_flight_computers(commandline_args(), timestep)
leader = flight_computers[0]

state = readout_state(timestep)
timestep += 1

r = call_peer('http://127.0.0.1:5000',
              'acceptable_action',
              action=leader.sample_next_action())

r = call_peer('http://127.0.0.1:5000',
              'decide_on_action',
              action=leader.sample_next_action())

print(r)
