import requests
import json
from flask import Flask, request
from server import LogEntry, ServerLog, Server, ServerEncoder

from starter_code.withoutksp import allocate_flight_computers, commandline_args, readout_state

from starter_code.computers import FlightComputer
from utils import BASE_URL,call_peer, NetworkException

server = Server("one")

class FlightComputerNet(FlightComputer):

    def __init__(self, state):
        super(FlightComputerNet, self).__init__(state)

    def add_peer(self, peer_url):
        self.peers.append(peer_url)

    def decide_on_action(self, action):

        # Runnin process outside http req/repl
        #  Sequentially, should be in parallel !

        acceptations = 0
        for peer_url in self.peers:
            try:
                accept = call_peer(peer_url,
                                   'acceptable_action', action=action)
                if accept:
                    acceptations += 1
            except NetworkException:
                pass

        decided = acceptations / (len(self.peers) + 1) > 0.5

        if decided:
            for p in self.peers:
                p.deliver_action(action)
                self.deliver_action(action)

        return decided


flight_computer = FlightComputerNet(readout_state(0))

app = Flask(__name__)

@app.route("/appendEntries", methods=('PUT',))
def appendEntriesWeb():
    if request.method == 'PUT':
        return json.dumps(server.log, cls=ServerEncoder)
        #return "zuliu" + request.form['term']

@app.route("/decide_on_action", methods=('PUT',))
def decide_on_action():
    if request.method == 'PUT':
        r = flight_computer.decide_on_action(
            json.loads(request.form['action']))
        return json.dumps(r)


@app.route("/acceptable_action", methods=('PUT',))
def acceptable_action():
    if request.method == 'PUT':
        p = json.loads(request.form['action'])
        r = flight_computer.acceptable_action(p)
        return json.dumps(r)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True, help="Port")
    parser.add_argument("--peers", type=int, default=2, help="Total peer number (this server includes)")
    parser.add_argument("--computer", type=str, help="Type fo computer FlightComputer")
    args, _ = parser.parse_known_args()

    for i in range(args.peers):
        port = 5000 + i
        if port != args.port:
            flight_computer.add_peer(f"{BASE_URL}:{port}")

    app.run(port=args.port)
