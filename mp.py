import traceback
import logging
import random
from time import sleep
from datetime import datetime

from multiprocessing import Process, Queue
from queue import Empty


from starter_code.withoutksp import allocate_flight_computers, commandline_args, readout_state

from server import Server

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
            import sys, traceback
            print('Whoops! Problem:', file=sys.stderr)
            traceback.print_exc(file=sys.stderr)

        except KeyboardInterrupt:
            break

    fo.flush()
    fo.close()



class Worker(Process):

    def run(self):
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

                    if True or 'STATE' not in msg:
                        self.log(msg)

                except Empty:
                    self.log("Empty queue ?")

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



    def send_me_leader(self, name):
        self._leader_queue.put({"type" : "LEADER_ANNONCE",
                                "name" : name})

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

    def set_raft_server(self, s):
        self._raft_server = s



if __name__ == '__main__':
    # from multiprocessing import set_start_method
    # set_start_method("spawn")

    logging_queue = Queue()
    log_listener = Process(target=logger_process,
                           args=(logging_queue,))
    log_listener.start()

    flight_computers = allocate_flight_computers(commandline_args(),timestep=0)

    logging.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%H:%M:%S')

    jobs = []
    jobs_queue = dict()
    control_queue = dict()

    leader_queue = Queue()

    raft_server = Server("Test1")

    for i in range(len(flight_computers)):

        recq = Queue()
        # recq, qs, c, lq, logq
        p = Worker(name=f"Computer {i}")

        jobs_queue[p] = recq
        control_queue[p] = Queue()

        p.set_raft_server(raft_server)
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

        print(f"For sender {sender.name}, receivers queues for {send_queues.keys()}")
        sender.set_sending_queues(send_queues)

    for j in jobs:
        j.start()


    start_time = datetime.now()
    timestep = 0
    old_time_step = -1
    old_state = None

    try:
        while True:
            sleep(0.00005)

            delta = datetime.now() - start_time
            timestep = int((delta.seconds + delta.microseconds/1000000) / 0.0001)

            if timestep > old_time_step:
                old_time_step = timestep

                # Read spaceships instruments
                try:
                    state = readout_state(timestep)
                except IndexError:
                    # No more instrument data, end of experiment,
                    break

                if timestep % 10000 < 3 and old_state != state:
                    print(f"TICK {timestep}/84000 {state}")

                if 3400 < timestep < 3440:
                    to_kill = jobs[0]

                    if to_kill.is_alive():
                        logging.warning(f"Crashing job '{to_kill.name}'")
                        jobs_queue[to_kill].put("STOP", block=True)
                        logging.warning(f"Crashed job '{to_kill.name}'")


                # Inform the computer(s)
                if old_state != state:
                    old_state = state
                    for job, sendq in jobs_queue.items():
                        if job.is_alive():
                            sendq.put(["STATE", state], block=True)

            try:
                # 1/ Le leader est élu : leader_queue.put("LEADER_ANNOUCNCE : I am the leader")
                # 2/ Le client notr le nom_du_leader
                # 3/ Le client fait uen demande au leader : server[nom_du_leader].put()
                # 4/ Le leader annonce sa décision : leader_queue.put("DECISION : dkjfksljdf")

                action = leader_queue.get(block=False)

                if action['type'] == "LEADER_ANNONCE":
                    # change leader
                    pass
                elif action['type'] == "DECISION":
                    # do action
                    pass

            except Empty:
                pass

    except KeyboardInterrupt:
        pass

    logging.info("Experiment complete")

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
    while not logging_queue.empty():
        logging_queue.get(block=True)
    logging_queue.close()

    logging.info("emptying leader queue")
    while not leader_queue.empty():
        leader_queue.get(block=True)
    leader_queue.close()

    logging.info("Joining processes")
    for j in jobs:
        j.join()
    log_listener.join()

    logging.info("Done")
    exit()
