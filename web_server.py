import requests
import json
from flask import Flask, request
from server import LogEntry, ServerLog, Server, ServerEncoder

from starter_code.withoutksp import allocate_flight_computers, commandline_args, readout_state

from starter_code.computers import FlightComputer

server = Server("one")

class FlightComputerNet(FlightComputer):

    def call_peer(self, peer_url, params*):
        r = requests.put('http://127.0.0.1:5000/acceptable_state',
                         data = {"state" : json.dumps(state)})
        return

    def add_peer(self, peer_url):
        self.peers.append(peer_url)

    def decide_on_state(self, state):

        #  Sequentially, should be in parallel !
        for peer_url in self.peers:
            self.peer_call('acceptable_state', state=json.dumps(state))

        acceptations = [p.acceptable_state(state) for p in self.peers]
        decided = sum(acceptations) / (len(self.peers) + 1) > 0.5

        if decided:
            for p in self.peers:
                p.deliver_state(state)
            self.deliver_state(state)

        return decided


flight_computer = FlightComputerNet()

app = Flask(__name__)

@app.route("/appendEntries", methods=('PUT',))
def appendEntriesWeb():
    if request.method == 'PUT':
        return json.dumps(server.log, cls=ServerEncoder)
        #return "zuliu" + request.form['term']

@app.route("/decideOnState", methods=('PUT',))
def decideOnState():
    if request.method == 'PUT':
        print(json.loads(request.form['state']))
        return json.dumps(server.log, cls=ServerEncoder)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True, help="Port")
    parser.add_argument("--computer", type=str, help="Type fo computer FlightComputer")
    args, _ = parser.parse_known_args()

    app.run(port=args.port)
