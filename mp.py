import sys
import enum
import traceback
import logging
import random
from time import sleep
from datetime import datetime

from multiprocessing import Process, Queue
from queue import Empty


from starter_code.withoutksp import allocate_flight_computers, commandline_args, readout_state

from starter_code.withoutksp import actions as expected_actions
from server import Server

class ClientStates(enum.Enum):
    Idle = 1
    SamplingAction = 2
    SampledAction = 3
    DecidedState = 4
    DecidedAction = 4

def logger_process(queue):
    fo = open("log","w")

    while True:
        try:
            record = queue.get()
            if record is None:
                # We send this as a sentinel to tell the listener to quit.
                break

            #print(f"{record}\n")
            fo.write(f"{record}\n")

        except Exception:
            print('Whoops! Problem:', file=sys.stderr)
            traceback.print_exc(file=sys.stderr)

        except KeyboardInterrupt:
            break

    fo.flush()
    fo.close()



class Worker(Process):

    def run(self):
        self._raft_server = Server(self.name, self._computer, self._logging)
        self._raft_server.set_comm(self)
        self._raft_server.start()

        try:
            while True:
                try:
                    # 1/Code Execution
                    # e.g. requestVote to all peers
                    #self.send_all("lklk")

                    # 2/ Message handling
                    msg = self._recq.get(block=False, timeout=1)
                    # if msg['method'] == "requestVote":
                    #     raft.server.requestVote()

                    # One can simulate a crash like this :
                    #raise Exception("crash")
                    # Now process the message

                    if type(msg) == dict and 'method' in msg:
                        if msg['method'] == "requestVote":
                            self._raft_server.requestVote(
                                msg['term'], msg['candidateId'],
                                msg['lastLogIndex'], msg['lastLogTerm'])

                        if msg['method'] == "requestVoteAck":
                            self._raft_server.requestVoteAck(
                                msg['term'], msg['success'],
                                msg['senderID'])

                        if msg['method'] == "appendEntries":
                            self._raft_server.appendEntries(
                                msg['term'], msg['leaderID'], msg['prevLogIndex'],
                                msg['prevLogTerm'], msg['entries'], msg['leaderCommit'])

                        if msg['method'] == "appendEntriesAck":
                            self._raft_server.appendEntriesAck(
                                msg['term'], msg['success'], msg['senderID'])

                        if msg['method'] == "proposeStateAction":
                            self._raft_server.proposeStateAction(
                                msg['state_action'])

                        elif msg['method'] == 'send_sample_next_action':
                            self._raft_server.sample_next_action()

                        elif msg['method'] == "decide_on_state":
                            self._raft_server.decide_on_state(
                                msg['state'])



                    # if 'STATE' not in msg:
                    #     self.log(msg)




                except Empty:
                    pass
                    #self.log("Empty queue ?")

                try:
                    #sleep(0.001)
                    if not self._control_queue.empty():
                        control = self._control_queue.get(block=False)
                        if control:
                            self.log(f"Got stop control : {control}")
                            break
                except Exception as ex:
                    self.log(f"Control {type(ex)} ?")


        except Exception as exception:
            self.log(f"Stopping '{self.name}' because {str(exception)}")
            self.log(str(traceback.format_exc()))

        except BaseException as exception:
            # KeyboardInterrupt is BaseException not Exception !
            self.log(f"Stopping '{self.name}' because {type(exception)}")

        self._raft_server.stop()
        while not self._recq.empty():
            self._recq.get()
        while not self._control_queue.empty():
            self._control_queue.get()


    def send_requestVote(self, peer_name, term, candidateID, lastLogIndex, lastLogTerm):
        d = {"method": "requestVote", "term": term, "candidateID": candidateID,
             "lastLogIndex": lastLogIndex, "lastLogTerm": lastLogTerm}
        self._sendqs[peer_name].put(d)

    def send_requestVoteAck(self, peer_name, term, success, senderID):
        d = {"method" : "requestVoteAck",
             "term": term, "success": success, "senderID": senderID}
        self._sendqs[peer_name].put(d)

    def send_appendEntries(self, peer_name, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
        d = {"method": "appendEntries", "term": term, "leaderID": leaderID,
             "prevLogTerm": prevLogTerm, "prevLogIndex": prevLogIndex,
             "entries": entries, "leaderCommit": leaderCommit}
        self.sendqs[peer_name].put(d)

    def send_appendEntriesAck(self, peer_name, term, success, senderID):
        d = {"method" : "appendEntriesAck",
             "term": term, "success": success, "senderID": senderID}
        self._sendqs[peer_name].put(d)

    def send_me_leader(self, name):
        self._leader_queue.put({"type" : "LEADER_ANNONCE",
                                "name" : name})

    def send_sampled_action(self, action):
        self._leader_queue.put({"type" : "SAMPLED_ACTION",
                                "action" : action})

    def send_decided_action(self, action_decided):
        self._leader_queue.put({"type" : "DECISION",
                                "action_decided" : action_decided})

    def send_decided_action(self, action_decided):
        self._leader_queue.put({"type" : "DECISION",
                                "action_decided" : action_decided})

    def send_decided_state(self, state_decided):
        self._leader_queue.put({"type" : "DECISION_STATE",
                                "state_decided" : state_decided})

    def log(self, msg):
        #return
        assert msg is not None

        self._logging.put(f"{datetime.now()} {self.name}: {msg}")

    def send_all(self, msg):
        for dest, q in self._sendqs.items():
            self.log(f"To {dest} : {msg}")
            q.put(msg)

    def send_msg(self, dest, msg):
        self.log(f"To {dest} : {msg}")
        self._sendqs[dest].put(msg)

    def set_receiving_queue(self, q):
        self._recq = q

    def set_sending_queues(self, qs):
        self._sendqs = qs

    def set_computer(self, c):
        self._computer = c

    def set_leader_queue(self, q):
        self._leader_queue = q

    def set_logging_queue(self, q):
        self._logging = q

    def set_controle_queue(self, q):
        self._control_queue = q



def send_propose_state_action(job_queue, state, action):
    job_queue.put({"method" : "proposeStateAction",
                   "state_action" : (state, action)})

def send_sample_next_action(job_queue):
    job_queue.put({"method" : "send_sample_next_action"})


def send_decide_on_state(job_queue, state):
    job_queue.put({"method" : "decide_on_state",
                   "state" : state})

def init_logging():
    logging_queue = Queue()
    log_listener = Process(target=logger_process,
                           args=(logging_queue,))
    log_listener.start()


    logging.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%H:%M:%S')

    return logging_queue, log_listener



def shutdown_logging():
    while not logging_queue.empty():
        logging_queue.get(block=True)
    logging_queue.close()
    logging.info("Done")


def init_workers(flight_computers, logging_queue):
    jobs = []
    jobs_queue = dict()
    control_queue = dict()

    leader_queue = Queue()

    for i in range(len(flight_computers)):

        recq = Queue()
        # recq, qs, c, lq, logq
        p = Worker(name=f"Computer {i}")

        jobs_queue[p] = recq
        control_queue[p] = Queue()

        p.set_computer(flight_computers[i % len(flight_computers)])
        p.set_leader_queue(leader_queue)
        p.set_logging_queue(logging_queue)

        # Set destination queue
        p.set_receiving_queue(jobs_queue[p])
        p.set_controle_queue(control_queue[p])

        jobs.append(p)



    for sender in jobs:
        send_queues = dict()

        for receiver in jobs:
            if sender != receiver:
                send_queues[receiver.name] = receiver._recq

        #print(f"For sender {sender.name}, receivers queues for {send_queues.keys()}")
        sender.set_sending_queues(send_queues)


    return jobs, jobs_queue, control_queue, leader_queue


def shut_down_workers(jobs, control_queue, jobs_queue, leader_queue):
    # https://stackoverflow.com/questions/34506638/how-to-register-atexit-function-in-pythons-multiprocessing-subprocess

    # At this point, jobs may still be sending messages to each other
    # So what we do is we first read all messages to clear the queue
    # and put a "STOP" right after. We basically overflow each
    # process with stop message and prevent them of working. In
    # the end, that ought to work. Ideally we should have
    # a stop channel to complete this

    logging.info("Requesting process to stop")
    for j in jobs:
        if j.is_alive():
            control_queue[j].put("STOP")

    # Wait for control message to trigger process exit.
    # This will work only if the process can handle
    # the control message :-(

    while any([j.is_alive() for j in jobs]):
        sleep(1)

    # We clear all remaining messages.
    logging.info("Clearing messages")



    for j in jobs:
        jobs_queue[j].close()
        jobs_queue[j].join_thread()
        control_queue[j].close()
        control_queue[j].join_thread()

    logging.info("Killing logging process")
    logging_queue.put_nowait(None)
    while log_listener.is_alive():
        logging.info("Log listener alive")
        sleep(1)

    # shutdown_logging()

    logging.info("emptying leader queue")
    while not leader_queue.empty():
        leader_queue.get(block=True)
    leader_queue.close()

    logging.info("Joining processes")
    for j in jobs:
        j.join()
    log_listener.join()


def test_action(expected_actions, action, timestep):
    logging.debug(f"Tested action at {timestep} {action}")

    TOLERANCE = 100
    for i in range(2*TOLERANCE):
        fail = False
        expected_action = expected_actions[max(0, timestep+i-TOLERANCE)]
        for k in action.keys():
            if k in expected_action and action[k] != expected_action[k]:
                fail = True
                break

        if not fail:
            return True

    logging.error(f"Expected action wrong at time step {timestep}")
    return False


if __name__ == '__main__':
    # from multiprocessing import set_start_method
    # set_start_method("spawn")

    logging.basicConfig(level=logging.WARNING)

    logging_queue, log_listener = init_logging()

    flight_computers = allocate_flight_computers(commandline_args(),timestep=0)

    jobs, jobs_queue, control_queue, leader_queue = init_workers(flight_computers, logging_queue)

    for j in jobs:
        j.start()


    start_time = datetime.now()
    timestep = 0
    old_time_step = -1
    old_state = None

    last_log_timestep = 0
    good_decisions = 0

    current_leader = jobs[0]
    decided_state, sample_action, timestep_sm = None, None, None

    client_state = ClientStates.Idle

    logging.info("Clear for take off")

    TIMESTEP = 0.001
    try:
        while True:
            sleep(0.0005)

            delta = datetime.now() - start_time
            timestep = int((delta.seconds + delta.microseconds/1000000) / TIMESTEP)

            if timestep > old_time_step:
                old_time_step = timestep

                # Read spaceships instruments
                try:
                    state = readout_state(timestep)
                except IndexError:
                    # No more instrument data, end of experiment,
                    break

            if current_leader:

                if client_state == ClientStates.Idle:
                    timestep_sm = timestep
                    logging.debug(f"[{timestep} - {timestep_sm}] asking state decision : {state}")
                    send_decide_on_state(jobs_queue[current_leader], state)

                    client_state = ClientStates.SamplingAction


            try:
                # 1/ Le leader est élu : leader_queue.put("LEADER_ANNOUCNCE : I am the leader")
                # 2/ Le client notr le nom_du_leader
                # 3/ Le client fait uen demande au leader : server[nom_du_leader].put()
                # 4/ Le leader annonce sa décision : leader_queue.put("DECISION : dkjfksljdf")

                action = leader_queue.get(block=False)

                if action['type'] == "LEADER_ANNONCE":
                    # change leader
                    for j in jobs:
                        if j.name == action['name']:
                            current_leader = j
                            logging.warning(f"[{timestep}] New leader elected: {action['name']}")
                    # Reset state machine
                    client_state = ClientStates.Idle
                    decided_state, sample_action, timestep_sm = None, None, None

                elif action['type'] == "DECISION_STATE":
                    logging.debug(f"[{timestep} - {timestep_sm}] Decided state : {action}")

                    client_state = ClientStates.DecidedState
                    decided_state = action['state_decided']

                    send_sample_next_action(jobs_queue[current_leader])

                elif action['type'] == "SAMPLED_ACTION":
                    logging.debug(f"[{timestep} - {timestep_sm}] Sampled action : {action['action']}")

                    client_state = ClientStates.SampledAction
                    sample_action = action['action']

                    send_propose_state_action(
                        jobs_queue[current_leader],
                        decided_state, sample_action)

                elif action['type'] == "DECISION":
                    logging.debug(f"[{timestep} - {timestep_sm}] Decision taken : {action}")
                    # do action

                    if action['action_decided']:
                        if not test_action(expected_actions, action['action_decided'], timestep):
                            logging.error(f"[{timestep} - {timestep_sm}] Decision taken : {action}")
                        else:
                            good_decisions += 1


                    # Reset state machine
                    client_state = ClientStates.Idle
                    decided_state, sample_action, timestep_sm = None, None, None


            except Empty:
                pass


            if timestep > last_log_timestep + 1/TIMESTEP:
                last_log_timestep = timestep

                dps = (good_decisions / timestep) / TIMESTEP
                logging.error(f"[{timestep}] Progress good decisions={good_decisions} ({dps:.1f} per second)")

    except KeyboardInterrupt:
        pass

    except Exception as ex:
        logging.error(str(ex))
        logging.error(str(traceback.format_exc()))

    logging.info("Experiment complete")

    shut_down_workers(jobs, control_queue, jobs_queue, leader_queue)
    shutdown_logging()
    exit()
