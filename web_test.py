import json
import time
import requests
from starter_code.withoutksp import allocate_flight_computers, commandline_args, readout_state




RETRY_TIME = 10 # seconds




r = None
timestep = 1

while True:
    try:
        state = readout_state(timestep)
        timestep += 1

        print(state)

        r = requests.put('http://127.0.0.1:5000/decideOnState',
                         data = {"state" : json.dumps(state)})

        # r = requests.put('http://127.0.0.1:5000/appendEntries',
        #                  data = { "term" : "ze term",
        #                           "leaderId" : "EEE",
        #                           "prevLogIndex" : "12",
        #                           "prevLogTerm" : "fd",
        #                           "entries" : "",
        #                           "leaderCommit" : ""})
        if r.status_code == 200:
            print(r.text)
            break
        else:
            raise Exception("Bad status code")

    except Exception as ex:
        print(ex)
        time.sleep(RETRY_TIME)
