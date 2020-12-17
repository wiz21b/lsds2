import time
import threading
import logging
import requests
import json
from flask import Flask, request
from server import LogEntry, ServerLog, Server, ServerEncoder

from starter_code.withoutksp import allocate_flight_computers, commandline_args, readout_state

from starter_code.computers import FlightComputer
from utils import BASE_URL, call_peer, NetworkException, call_peer_with_dict, ThreadWithReturnValue



raft_server = Server("one")
raft_server.start_timer(2)

class FlightComputerNet(FlightComputer):

    def __init__(self, state):
        super(FlightComputerNet, self).__init__(state)

    def add_peer(self, peer_url):
        self.peers.append(peer_url)


    def decide_on_action(self, action):

        # Runnin process outside http req/repl
        #  Sequentially, should be in parallel !

        calls = [ThreadWithReturnValue(
            target=call_peer_with_dict,
            args=(peer_url, 'acceptable_action', {"action": action}))
                  for peer_url in self.peers]
        [t.start() for t in calls]

        results = [t.join() for t in calls]
        acceptations = sum(filter(lambda t: t, results))

        # This code below explains the code above...

        # acceptations = 0
        # for peer_url in self.peers:
        #     try:
        #         accept = call_peer(peer_url,
        #                            'acceptable_action', action=action)
        #         if accept:
        #             acceptations += 1
        #     except NetworkException:
        #         pass

        app.logger.info(f"{acceptations} acceptations")
        decided = acceptations / (len(self.peers) + 1) > 0.5

        if decided:
            for peer_url in self.peers:
                call_peer(peer_url,
                          'deliver_action', action=action)

            self.deliver_action(action)

        return decided


    def decide_on_state(self, state):

        acceptations = 0
        for peer_url in self.peers:
            try:
                accept = call_peer(peer_url,'acceptable_state', state=state)

                if accept:
                    acceptations += 1
            except NetworkException:
                pass

        app.logger.info(f"{acceptations} acceptations")
        decided = acceptations / (len(self.peers) +1) > 0.5

        if decided:
            for peer_url in self.peers:
                call_peer(peer_url, 'deliver_state', state=state)
            self.deliver_state(state)

        return decided


    def debug(self):
        return {"State" : f"{self.state}",
                "next_action" : f"{self.sample_next_action()}",
                "stage" : self.current_stage_index}


flight_computer = FlightComputerNet(readout_state(0))


app = Flask(__name__)

@app.route("/appendEntries", methods=('PUT',))
def appendEntriesWeb():
    if request.method == 'PUT':
        return json.dumps(raft_server.log, cls=ServerEncoder)
        #return "zuliu" + request.form['term']

@app.route("/requestVote", methods=('PUT',))
def requestVote():
    if request.method == 'PUT':
        with raft_server._thread_lock:
            res = json.dumps(raft_server.requestVote(), cls=ServerEncoder)
        return res


@app.route("/decide_on_action", methods=('PUT',))
def decide_on_action():
    if request.method == 'PUT':
        r = flight_computer.decide_on_action(
            json.loads(request.form['action']))
        return json.dumps(r)

@app.route("/sample_next_action", methods=('PUT',))
def sample_next_action():
    if request.method == 'PUT':
        r = flight_computer.sample_next_action()
        return json.dumps(r)


@app.route("/decide_on_state", methods=('PUT',))
def decide_on_state():
    if request.method == 'PUT':
        r = flight_computer.decide_on_state(
            json.loads(request.form['state']))
        return json.dumps(r)

@app.route("/deliver_action", methods=('PUT',))
def deliver_action():
    if request.method == 'PUT':
        r = flight_computer.deliver_action(
            json.loads(request.form['action']))
        return json.dumps(r)

@app.route("/deliver_state", methods=('PUT',))
def deliver_state():
    if request.method == 'PUT':
        r = flight_computer.deliver_state(json.loads(request.form['state']))
        return json.dumps(r)

@app.route("/acceptable_action", methods=('PUT',))
def acceptable_action():
    if request.method == 'PUT':
        p = json.loads(request.form['action'])
        r = flight_computer.acceptable_action(p)
        return json.dumps(r)

@app.route("/acceptable_state", methods=('PUT',))
def acceptable_state():
    if request.method == 'PUT':
        p = json.loads(request.form['state'])
        r = flight_computer.acceptable_state(p)
        return json.dumps(r)

@app.route("/debug", methods=('PUT',))
def debug():
    if request.method == 'PUT':
        return json.dumps(flight_computer.debug())

# https://www.edureka.co/community/30828/how-do-you-add-a-background-thread-to-flask-in-python
# def server_thread():


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True, help="Port")
    parser.add_argument("--peers", type=int, default=2, help="Total peer number (this server includes)")
    parser.add_argument("--computer", type=str, help="Type fo computer FlightComputer")
    args, _ = parser.parse_known_args()

    assert 5000 <= args.port <= 5000 + args.peers

    for i in range(args.peers):
        port = 5000 + i
        if port != args.port:
            flight_computer.add_peer(f"{BASE_URL}:{port}")
            raft_server.add_peer(f"{BASE_URL}:{port}")

    logging.getLogger('werkzeug').disabled = True

    #app.logger.setLevel(logging.DEBUG)
    app.run(port=args.port)
