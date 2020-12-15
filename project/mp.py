import logging
import random
from time import sleep
from multiprocessing import Process, Queue
from queue import Empty

logging.basicConfig(
    format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
    level=logging.DEBUG,
    datefmt='%H:%M:%S')

class Worker(Process):

    def run(self):
        for i in range(random.randint(10, 20)):
            sleep(random.random())

            dest = random.choice([v for v in self._sendqs.keys()])
            msg = [dest, f"Sending from {self.name} to {dest}"]
            self.send_msg(dest, msg)

            try:
                msg = self._recq.get(block=False)
            except Empty:
                logging.error(f"{self.name} hello")
                pass

        while not self._recq.empty():
            msg = self._recq.get(block=False)
            print(f"{self.name} received : {msg}")

    def send_msg(self, dest, msg):
        logging.info(f"To {dest} : {msg}")
        self._sendqs[dest].put(msg)


    def set_receiving_queue(self, q):
        self._recq = q

    def set_sending_queues(self, qs):
        self._sendqs = qs

if __name__ == '__main__':

    jobs = []
    jobs_queue = dict()

    for i in range(5):
        p = Worker(name=f"Computer {i}")
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

    for j in jobs:
        j.join()

    print("Done")
