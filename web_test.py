import time
import requests

RETRY_TIME = 10 # seconds

r = None
while True:
    try:
        r = requests.put('http://127.0.0.1:5000/appendEntries',
                         data = { "term" : "ze term",
                                  "leaderId" : "EEE",
                                  "prevLogIndex" : "12",
                                  "prevLogTerm" : "fd",
                                  "entries" : "",
                                  "leaderCommit" : ""})
        if r.status_code == 200:
            print(r.text)
            break
        else:
            raise Exception("Bad status code")

    except Exception as ex:
        print(ex)
        time.sleep(RETRY_TIME)
