import threading
from datetime import datetime
import json
import random
import math
from heapq import nlargest
from utils import call_peer,ThreadWithReturnValue,call_peer_with_dict


def time_out_action(raft_server):
    with raft_server.thread_lock:
        raft_server.election_time_out()


class ServerEncoder(json.JSONEncoder):
    def default( self, obj):
        if isinstance(obj, LogEntry):
            return { "command" : obj.command, "term" : obj.term }
        if isinstance(obj, ServerLog):
            return obj._data

        return json.JSONEncoder.default(self, obj)



class LogEntry:
    def __init__(self, command, term):
        self.command = command
        self.term = term

class AckEntry:
    def __init__(self, term, success, lastIndex=None):
        self.term = term
        self.success = success
        self.lastIndex = lastIndex

class ServerLog:
    def __init__(self, entries = []):
        self._data=[e for e in entries]

    def __getitem__(self, ndx):
        assert 0 <= ndx <= len(self._data)
        if ndx == 0:
            return LogEntry(None, 0)
        return self._data[ndx-1]

    def getItemFrom(self, ndx):
        assert 1 <= ndx <= len(self._data)
        if ndx == 1:
            ls = [LogEntry(None, 0)]
            ls = ls + self._data[ndx-1:]
            return ls

        return self._data[ndx-1:]

    def clear_from( self, ndx):
        assert 1 <= ndx <= len(self._data)
        self.data = self._data[ndx-1:] #C'est pas un clear to ndx et pas from ndx là ?

    def has_entry(self, entry):
        for e in self._data:
            if e.term == entry.term and e.command == entry.command:
                return True
        return False

    def append_entry(self, entry):
        assert isinstance(entry, LogEntry)
        self._data.append(entry)

    def lastIndex(self):
        return len(self._data)

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return self._data.iter()

    def enumerate(self):
        n = 1
        for entry in self._data:
            yield n, self.data[n-1]
            n += 1


class Server:
    def __init__(self, name, flight_computer, log, isLocal = False):
        self._thread_lock = threading.Lock()
        self._logging = log
        self.flight_computer = flight_computer
        self.name = name
        self.isLocal = isLocal

        self.currentTerm = 0
        self.votedFor = None
        #Put ID to commands
        self.log = ServerLog() # An array [1..]

        # self.log[1] = LogEntry(action, term)
        # self.log[45].term .action

        self.commitIndex = 0
        self.lastApplied = 0

        self.reset_leader_state()
        self.state = "Follower"
        self.timeout = 0
        self.stepDown = False

        self.peers = dict()

        self.ackEntries = dict()
        self.ackElec = dict()
        self._timer_thread = None

        self.heartBeatLen = 5
        self.start_timer(self.random_timer_init())
        self._timeout_expired = False

        self._heartbeat_timer_thread = threading.Timer(self.heartBeatLen, self.heartbeat_callback)
        self._heartbeat_timer_thread.start()

    def logger(self, msg):
        #return
        assert msg is not None

        self._logging.put(f"{datetime.now()} {self.name}: {msg}")

    def heartbeat_callback(self):
        print("hb of " + self.name + " occured")
        # self._heartbeat_timer_thread.join()

        #with self._heartbeat_timer_thread:

        if self.state == "Leader":
            print("hb of " + self.name + " executed because he is leader")

            if self.isLocal:
                for _, server in self.peers.items():
                    server.appendEntries(self.currentTerm, self.name, None, None, None, self.commitIndex)
            else:
                pass
                #self.comm.send_all(appendEntries(self.currentTerm, self.name, None, None, None, self.commitIndex))

        self._heartbeat_timer_thread = threading.Timer(self.heartBeatLen, self.heartbeat_callback)
        self._heartbeat_timer_thread.start()

    def random_timer_init(self):
        #Timeout can't be less than 2 heartbeat periods to avoid too frequent election request simply beacause of packets drop
        value = self.heartBeatLen * 2
        #Add to the prev value a percentage of the time of 2 heartbeat periods
        value += (random.random() * (self.heartBeatLen * 2))
        return value

    def start_timer(self, duration):
        if self._timer_thread:
            self._timer_thread.cancel()
        else:
            print("Timeout de " + str(self.name) + " reset à: " + str(duration))

        self._timer_thread = threading.Timer(duration, self.timeout_callback)

        self._timer_thread.start()

        self._timeout_expired = False

    def start(self):
        self.start_timer(4)

    def stop(self):
        # Cancels are necessary to properly stop the timer
        # (and release thread resources)
        if self._timer_thread:
            self._timer_thread.cancel()
            self._timer_thread.join()
        if self._heartbeat_timer_thread:
            self._heartbeat_timer_thread.cancel()
            self._heartbeat_timer_thread.join()



    def set_comm(self, worker):
        self.comm = worker

    def add_peer(self, peerID, peer_url):
        self.peers[peerID] = peer_url
        self.ackEntries[peerID] = AckEntry(None, None)
        self.ackElec[peerID] = AckEntry(None, None)

    def sample_next_action(self):
        # The flight computer of the *leader* selects the action
        # he'd do, based on the state that was
        # commited by RAFT (but without asking other computers
        # (yet)).

        action = self.flight_computer.sample_next_action()
        self.comm.send_sampled_action(action)


    def decide_on_state(self, state):
        # The client requests the leader to apply RAFT
        # to decide on the give state
        state_decided = self.flight_computer.decide_on_state(state)
        self.comm.send_decided_state(state)



    def proposeStateAction(self, state_action):
        # Client proposes a state and action to the leader
        # so that it can decide if the cluster
        # accepts it.

        # Vasco's stuff

        state, action = state_action
        # Example code
        has_decided = self.flight_computer.decide_on_action(action)

        if has_decided:
            self.comm.send_decided_action(action)
        else:
            self.comm.send_decided_action(False)

    def timeout_callback(self):
        with self._thread_lock:
            # do stuff here

            #self.comm.send_me_leader(self.name)

            #send_all( { "methode" : "requestVote", param...} )

            self._timeout_expired = True

    def ack_entries_reset(self):
        for key, _ in self.peers.items():
            self.ackEntries[key] = AckEntry(None, None)

    def ack_elec_reset(self):
        for key, _ in self.peers.items():
            self.ackElec[key] = AckEntry(None, None)

    def convert_to_follower(self):
        self.state = "Follower"
        self.election_period_start = datetime.now()
        self.received_append_entries = 0

    def convert_to_candidate(self):
        self.state = "Candidate"
        self.currentTerm += 1
        self.votedFor = self.name
        self.received_append_entries = 0


        #send_all( { "methode" : "requestVote", param....} )

        # for peer_queues in self.peers.items():
        #     # RPC to send requestVote @ peer_url

        #         # quid des time out ?
        #         # quid des exceptions ?



    def convert_to_leader(self):
        pass

    def convert_to_candidate_step2(self, call_results):

        acceptations = sum(filter(lambda t: t, call_results))
        if acceptations > math.ceil(len(self.peers) / 2):
            self.convert_to_leader()
        elif self.received_append_entries > 0:
            self.convert_to_follower()


    # If timeout occurs, then thif method is called magic !
    def election_time_out(self):
        print("TIMEOUT")
        return

        if self.state == "Follower":
            if self.received_append_entries == 0:
                self.convert_to_candidate()
                return None, None
        elif self.state == "Candidate":
            # FIXME Start new election how to ?
            return None, None

    def reset_leader_state(self):
        self.nextIndex = dict()

        # self.nextIndex[ peer.name ] = 3

        self.matchIndex = dict()

    def persist_state(self):
        with open(self.name + ".txt") as fo:
            l = json.dumps(self, cls=ServerEncoder)
            fo.write(f"{self.currentTerm}\n{self.votedFor}\n{l}")

    def apply_state_machine(self, log_entry):
        pass

    def server_behaviou(self):
        # If commitIndex > lastApplied: increment lastApplied,
        # apply log[lastApplied] to state machine (§5.3)

        if self.commitIndex > self.lastApplied:
            self.lastApplied += 1

            self.apply_state_machine(self.log[self.lastApplied])

    def who_is_leader(self):
        if self.state == "Leader":
            return self
        # for _, server in self.peers.items():
        #     if server.state == "Leader":
        #         return server
        return None

    def print_log(self):
        roof = "######################################"
        print(f"{self.name} is a {self.state} ")
        if self.log.lastIndex() > 0:
            print(roof)
            elems = self.log.getItemFrom(1)
            for elem in elems:
                line = "Action: " + str(elem.command) + " - Term: " + str(elem.term)
                spacesLen = len(roof) - 2 - len(line)
                lSpaces = " " * math.floor(spacesLen/2)
                rSpaces = " " * math.ceil(spacesLen/2)
                print("#" + lSpaces + line + rSpaces + "#")
            print(roof)

    def all_server_update(self):
        if self.commitIndex > self.lastApplied:
            #apply self.log[lastApplied].command to state machine
            pass

        return

    def followers_update(self):
        #Become a candidate for term = self.currentTerm + 1
        if self.state == "Follower" and self._timeout_expired == True:
            self.state = "Candidate"
            self.currentTerm += 1
            self.votedFor = self.name
            #reset of random timeout
            self.start_timer(self.random_timer_init())

            #Send election RPC to all peers
            for _, server in self.peers.items():
                server.requestVote(self.currentTerm, self.name, self.log.lastIndex(), self.log.__getitem__(self.log.lastIndex()).term)

    def candidates_update(self):
        if self.state == "Candidate":
            nbVotes = 1
            for key, server in self.peers.items():
                if self.ackElec[key].success == True:
                    nbVotes += 1
                    self.currentTerm = max(self.currentTerm, self.ackElec[key].term)

            #Become the new leader
            if nbVotes > (len(self.peers) + 1) / 2:
                self.state = "Leader"
                self.votedFor = None
                self.reset_leader_state()
                for key, server in self.peers.items():
                    self.nextIndex[key] = self.log.lastIndex() + 1
                    self.matchIndex[key] = 0
                    #Leader send heartbeat RPC to all other servers to say he is the new leader
                    server.appendEntries(self.currentTerm, self.name, None, None, None, self.commitIndex)
                return
            '''
            if self.stepDown == True:
                self.state = "Follower"
                self.stepDown = False
                #reset of random timeout
                self.start_timer(self.random_timer_init())
                return
            '''
            if self._timeout_expired == True:
                self.currentTerm += 1
                #reset of random timeout
                self.start_timer(self.random_timer_init())

                #Send new election RPC to all peers (prevent tie or fail election to block the system)
                for _, server in self.peers.items():
                    server.requestVote(self.currentTerm, self.name, self.log.lastIndex(), self.log.__getitem__(self.log.lastIndex()).term)

    def leaders_update(self):
        if self.state == "Leader":
            if self._timeout_expired == True:
                self._timeout_expired == False
            '''
            if self.stepDown == True:
                self.state = "Follower"
                self.stepDown = False
                #reset of random timeout
                self.start_timer(self.random_timer_init())
                return
            '''
            for key, server in self.peers.items():
                if self.log.lastIndex() >= self.nextIndex[key]:
                    entries = self.log.getItemFrom(self.nextIndex[key])
                    server.appendEntries(self.currentTerm, self.name, self.nextIndex[key]-1,
                                            entries[0].term, entries, self.commitIndex)

            for key, _ in self.peers.items():
                if self.ackEntries[key].success == True:
                    self.matchIndex[key] = self.ackEntries[key].lastIndex
                    self.nextIndex[key] = self.log.lastIndex() + 1
                    self.currentTerm = max(self.currentTerm, self.ackEntries[key].term)
                    self.ackEntries[key].success = None
                    self.ackEntries[key].term = None

                if self.ackEntries[key].success == False:
                    self.nextIndex[key] -= 1
                    self.currentTerm = max(self.currentTerm, self.ackEntries[key].term)
                    self.ackEntries[key].success = None
                    self.ackEntries[key].term = None

            nLargestIndex = nlargest(math.ceil((len(self.peers) + 1) / 2), self.matchIndex, key=self.matchIndex.get)
            nThLargestIndex = nLargestIndex[len(nLargestIndex)-1]

            if self.matchIndex[nThLargestIndex] > self.commitIndex and self.log[self.matchIndex[nThLargestIndex]].term == self.currentTerm:
                self.commitIndex = self.matchIndex[nThLargestIndex]

    def global_update(self):
        self.all_server_update()
        self.followers_update()
        self.candidates_update()
        self.leaders_update()

    #test t-bow
    def requestVote(self, term, candidateId, lastLogIndex, lastLogTerm):
        print(self.name, "requestVote dans server.py")
        if term < self.currentTerm:
            if self.isLocal:
                #print("Vote NOT Granted 1")
                self.peers[candidateId].requestVoteAck(self.currentTerm, False, self.name)
            else:
                self.comm.send_requestVoteAck(self.peers[candidateId] , self.currentTerm, False, self.name)
            return

        if self.currentTerm < term: 
            self.votedFor = None
            self.currentTerm = term

        if self.votedFor in (None, candidateId) and \
            lastLogIndex >= self.log.lastIndex():
            #print("Vote Granted")
            self.votedFor = candidateId
            self.state = "Follower"

            if self.isLocal:
                self.peers[candidateId].requestVoteAck(term, True, self.name)
            else:
                self.comm.send_requestVoteAck(self.peers[candidateId] , term, True, self.name)
            return

        if self.isLocal:
            #print("Vote NOT Granted 2")
            self.peers[candidateId].requestVoteAck(term, False, self.name)
        else:
            self.comm.send_requestVoteAck(self.peers[candidateId], term, False, self.name)

    #Vasco
    # CALL
    # def requestVote(self, term, candidateId, lastLogIndex, lastLogTerm):
    #     if term < self.currentTerm:
    #         self.peers[candidateId].requestVoteAck(self.currentTerm, False, self.name)

    #     self.currentTerm = term

    #     if self.votedFor in (None, candidateId) and \
    #         lastLogIndex >= self.log.lastIndex():
    #         self.peers[candidateId].requestVoteAck(term, True, self.name)

    #     self.peers[candidateId].requestVoteAck(term, False, self.name)

    # CALL
    def requestVoteAck(self, term, success, senderID):
        #print(self.name, "requestVoteAck dans server.py")
        self.ackElec[senderID].term = term
        self.ackElec[senderID].success = success



    def appendEntries(self, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
        #print(self.name, "appendEntries dans server.py")
        if term < self.currentTerm:
            if self.isLocal:
                self.peers[leaderId].appendEntriesAck(self.currentTerm, False, self.log.lastIndex(), self.name)
            else:
                self.comm.send_appendEntriesAck(self.peers[leaderId], self.currentTerm, False, self.log.lastIndex(), self.name)

            return

        if entries == None:
            #print("The RPC request received by " + self.name + " was an empty heartbeat")

            if leaderCommit > self.commitIndex:
                self.commitIndex = min(leaderCommit, self.log.lastIndex())

            #reset of random timeout
            self.start_timer(self.random_timer_init())

            if self.state == "Candidate" or self.state == "Leader":
                self.state = "Follower"

            self.currentTerm = term

            self.votedFor = None

            if self.isLocal:
                self.peers[leaderId].appendEntriesAck(term, True, self.log.lastIndex(), self.name)
            else:
                self.comm.send_appendEntriesAck(self.peers[leaderId], term, True, self.log.lastIndex(), self.name)
            return

        if prevLogIndex > self.log.__len__() or (prevLogIndex != 0 and self.log[prevLogIndex].term != prevLogTerm):
            if self.isLocal:
                self.peers[leaderId].appendEntriesAck(term, False, self.log.lastIndex(), self.name)
            else:
                self.comm.send_appendEntriesAck(self.peers[leaderId], term, False, self.log.lastIndex(), self.name)
            return

        i = prevLogIndex
        for ndx, _ in enumerate(entries):
            if i == 0:
                i += 1
                break

            if self.log.lastIndex() < i:
                break

            if self.log[i].term != entries[ndx].term:
                self.log.clear_from(i)
                # ndx is growing => if we clear from here there
                # is nothing left to delete from self.log
                break
            i += 1

        for ndx, _ in enumerate(entries):
            if ndx >= i - prevLogIndex:
                self.log.append_entry(entries[ndx])

        if leaderCommit > self.commitIndex:
            self.commitIndex = min(leaderCommit, self.log.lastIndex())

        #reset of random timeout
        self.start_timer(self.random_timer_init())

        if self.state == "Candidate" or self.state == "Leader":
            self.state = "Follower"

        self.currentTerm = term
        self.votedFor = None

        if self.isLocal:
            self.peers[leaderId].appendEntriesAck(term, True, self.log.lastIndex(), self.name)
        else:
            self.comm.send_appendEntriesAck(self.peers[leaderId], term, True, self.log.lastIndex(), self.name)

    # CALL
    def appendEntriesAck(self, term, success, lastIndex, senderID):
        #print(self.name, "appendEntriesAck dans server.py")
        self.ackEntries[senderID].term = term
        self.ackEntries[senderID].success = success
        self.ackEntries[senderID].lastIndex = lastIndex

if __name__ == '__main__':
    isLocal = True  #Mettre True pour tester avec tous les servs en local qui se connaissent, False pour utilisation des pipes

    nb_servers = 5
    all_servers = []
    for i in range(nb_servers):
        all_servers.append( Server(f"Serv{i+1}", None, None, isLocal))

    for server in all_servers:
        for peer in all_servers:
            if server != peer:
                server.add_peer(peer.name, peer)



    j = 1
    while True:

        #  Check leader election is OK

        # WARNING This is dangerous code:
        # IF server1 is leader and, while I'm counting the servers,
        # the server5 becomes the leader, then I will have wrongly
        # counter sever1 as a leader.

        leader = None
        nb_leaders = 0

        for s in all_servers:
            s._thread_lock.acquire()

        for s in all_servers:
            if s.who_is_leader():
                leader = s
                nb_leaders += 1

        for s in all_servers:
            s._thread_lock.release()

        if nb_leaders not in (0, 1):
            for s in all_servers:
                s.print_log()
                s.stop() # Clear threads
            print(f"ERROR {nb_leaders} leaders at the same time ?!")

            exit()


        # Show debugging info
        if j % 1000 == 0:
            #print(j)
            if leader: #add any condition at which an user input will be sent the the leader (c'est peut-être un peu trop de spam là)
                with leader._thread_lock:
                    actionID = leader.log.lastIndex()
                    action = "myRdmAction=" + str(actionID)
                    leader.log.append_entry(LogEntry(action, leader.currentTerm))

            if not j % 500000: #Thibault, ajuste ça à ta guise pour la fréquence d'affichage des log
                for s in all_servers:
                    s.print_log()

            if not leader and not j % 100000: #Ici aussi, juste comme tu le sens
                print("No leader so far")

        # Make sur the server run
        for s in all_servers:
            s.global_update()

        j += 1
