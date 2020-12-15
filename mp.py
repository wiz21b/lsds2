import logging
import random
from time import sleep
from datetime import datetime

from multiprocessing import Process, Queue
from queue import Empty

from starter_code.withoutksp import allocate_flight_computers, commandline_args, readout_state


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

    # try:
    #     while not queue.empty():
    #         queue.get(timeout=1)
    # except:
    #     pass

    fo.write("done")
    fo.flush()
    fo.close()



class Worker(Process):

    # def __init__(self, recq, qs, c, lq, logq):

    #     self._recq = recq
    #     self._sendqs = qs
    #     self._computer = c
    #     self._leader_queue = lq
    #     self._logging = logq

    #     super(Worker).__init__()


    def run(self):

        test_message = None

        try:
            while True:
                try:
                    if test_message is None:
                        #test_message = "done"
                        self.send_all("lklk")

                    msg = self._recq.get(block=True)

                    if msg == "STOP":
                        raise Exception("Requested stop")

                    # One can simulate a crash like this :
                    #raise Exception("crash")
                    # Now process the message

                    if 'STATE' not in msg:
                        self.log(msg)

                    sleep(0.5)

                except Empty:
                    self.log("Empty queue ?")

        except Exception as exception:
            self.log(f"Stopping '{self.name}' because {str(exception)}")
        except BaseException as exception:
            # KeyboardInterrupt is BaseException not Exception !
            self.log(f"Stopping '{self.name}' because {type(exception)}")

        finally:
            # Emptying the queue so that the process can stop
            pass

            # try:
            #     while not self._recq.empty():
            #         self._recq.get(block=True)
            # except:
            #     pass


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

if __name__ == '__main__':
    from multiprocessing import set_start_method
    set_start_method("spawn")

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

    leader_queue = Queue()

    for i in range(len(flight_computers)):

        recq = Queue()
        # recq, qs, c, lq, logq
        p = Worker(name=f"Computer {i}")

        jobs_queue[p] = recq
        p.set_computer(flight_computers[i])
        p.set_leader_queue(leader_queue)
        p.set_logging_queue(logging_queue)

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
                action = leader_queue.get(block=False)
            except Empty:
                pass

    except KeyboardInterrupt:
        pass

    logging.info("Experiment complete")

    # https://stackoverflow.com/questions/34506638/how-to-register-atexit-function-in-pythons-multiprocessing-subprocess


    logging.info("Sending STOP to computers")
    for job, sendq in jobs_queue.items():
        sendq.put("STOP", block=True)

    # At this point, jobs may still be sending messages to each other
    while any([j.is_alive() for j in jobs]):
        for j in jobs:
            while not jobs_queue[j].empty():
                jobs_queue[j].get(timeout=1)
            jobs_queue[j].put("STOP", block=True)
        logging.info("Jobs still alive")
        sleep(1)


    logging.info("Emptying computer queues")
    for job, sendq in jobs_queue.items():
        while not sendq.empty():
            sendq.get(block=True)

    logging.info("Killing logging")
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
        #j.terminate()
        j.join()
    log_listener.join()

    logging.info("Done")
    exit()
