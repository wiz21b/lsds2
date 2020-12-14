import logging
import random
from time import sleep
from datetime import datetime

from multiprocessing import Process, Queue
from queue import Empty

from starter_code.withoutksp import allocate_flight_computers, commandline_args, readout_state

class Worker(Process):

    def run(self):

        # dest = random.choice([v for v in self._sendqs.keys()])
        # msg = [dest, f"Sending from {self.name} to {dest}"]
        # self.send_msg(dest, msg)

        while True:
            while True:
                try:
                    msg = self._recq.get(block=False)
                    #logging.debug(msg)
                except Empty:
                    break


    def send_msg(self, dest, msg):
        logging.info(f"To {dest} : {msg}")
        self._sendqs[dest].put(msg)


    def set_receiving_queue(self, q):
        self._recq = q

    def set_sending_queues(self, qs):
        self._sendqs = qs

    def set_computer(self, c):
        self._computer = c

    def set_leader_queue(self, q):
        self._state_queue = q


if __name__ == '__main__':

    flight_computers = allocate_flight_computers(commandline_args(),timestep=0)

    logging.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%H:%M:%S')

    jobs = []
    jobs_queue = dict()

    leader_queue = Queue()

    for i in range(len(flight_computers)):
        p = Worker(name=f"Computer {i}")
        p.set_computer(flight_computers[i])
        p.set_leader_queue(leader_queue)

        jobs_queue[p] = Queue()
        p.set_receiving_queue(jobs_queue[p])
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

    while True:
        delta = datetime.now() - start_time
        timestep = int( (delta.seconds + delta.microseconds/1000000) / 0.001)
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

            # Inform the computer(s)
            if old_state != state:
                old_state = state
                for job, sendq in jobs_queue.items():
                    sendq.put(["STATE", state],block=False)

        try:
            action = leader_queue.get(block=False)
        except Empty:
            pass


    logging.info("Experiment complete, stopping computers")
    for j in jobs:
        j.terminate()

    logging.info("Done")
