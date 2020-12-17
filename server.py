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
        self.data = self.data[0:ndx-1] #C'est pas un clear to ndx et pas from ndx là ?

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
    def __init__(self, name, flight_computer):
        self._thread_lock = threading.Lock()

        self.flight_computer = flight_computer
        self.name = name

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
        self.start_timer(5)

        self._heartbeat_timer_thread = threading.Timer(1, self.heartbeat_callback)

    def heartbeat_callback(self):
        self._heartbeat_timer_thread.join()
        if self.state == "Leader":
            self._heartbeat_timer_thread = threading.Timer(1, self.heartbeat_callback)
            #self.comm.send_all(appendEntries(self.currentTerm, self.name, None, None, None, self.commitIndex))


    def start_timer(self, duration):
        if self._timer_thread:
            self._timer_thread.cancel()
        self._timer_thread = threading.Timer(duration, self.timeout_callback)

        self._timer_thread.start()

    def start(self):
        self.start_timer(4)

    def stop(self):
        if self._timer_thread:
            self._timer_thread.join()

    def set_comm(self, worker):
        self.comm = worker

    def init_timeout(self, value):
        self.timeout = value

    def add_peer(self, peerID, peer_url):
        self.peers[peerID] = peer_url
        self.ackEntries[peerID] = AckEntry(None, None)
        self.ackElec[peerID] = AckEntry(None, None)

    def sample_next_action(self):
        # The flight computer of the *leader* selects the action
        # he'd do, based on the state that was
        # commited by RAFT

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
        for server in self.peers:
            self.ackEntries[server] = AckEntry(None, None)

    def ack_elec_reset(self):
        for server in self.peers:
            self.ackElec[server] = AckEntry(None, None)

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

        # for peer_queues in self.peers:
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

    def all_server_update(self):
        if self.commitIndex > self.lastApplied:
            #apply self.log[lastApplied].command to state machine
            pass

        return

    def followers_update(self):
        if self.state == "Follower" and self.timeout == 0:
            self.state = "Candidate"
            self.currentTerm += 1
            self.votedFor = self.name
            #reset of random timeout /!\ current evalation is purely to not have 0 and is probably not efficient
            timeout = random.random()*10
            self.init_timeout(timeout)

            #Send election RPC to all peers
            for server in self.peers:
                server.requestVote(self.currentTerm, self.name, self.log.lastIndex(), self.log.__getitem__(self.log.lastIndex()).term)

    def candidates_update(self):
        if self.state == "Candidate":
            nbVotes = 1
            for server in self.peers:
                if self.ackElec[server].success == True:
                    nbVotes += 1
                    self.currentTerm = max(self.currentTerm, self.ackElec[server].term)

            if nbVotes > len(self.peers) + 1:
                for server in self.peers:
                    self.state = "Leader"
                    #Leader send heartbeat RPC to all other servers to say he is the new leader
                    server.appendEntries(self.currentTerm, self.name, None, None, None, self.commitIndex)

            if self.stepDown == True:
                self.state = "Follower"
                self.stepDown = False

            if self.timeout == 0:
                self.currentTerm += 1
                #reset of random timeout /!\ current evalation is purely to not have 0 and is probably not efficient
                timeout = random.random()*10
                self.init_timeout(timeout)

                #Send new election RPC to all peers (prevent tie or fail election to black the system)
                for server in self.peers:
                    server.requestVote(self.currentTerm, self.name, self.log.lastIndex(), self.log.__getitem__(self.log.lastIndex()).term)

    def leaders_update(self):
        if self.state == "Leader":
            actionID = str(int(random.random()*10))                 #Implement here an user action get
            action = "myRdmAction=" + actionID                      #Implement here an user action get
            self.log.append_entry(LogEntry(action, self.currentTerm))

            for server in self.peers:
                if self.log.lastIndex() >= self.nextIndex[server]:
                    entries = self.log.getItemFrom(self.nextIndex[server])
                    server.appendEntries(self.currentTerm, self.name, self.nextIndex[server]-1,
                                            entries[0].term, entries, self.commitIndex)

            for server in self.peers:
                if self.ackEntries[server].success == True:
                    self.matchIndex[server] = self.ackEntries[server].lastIndex
                    self.nextIndex[server] = self.log.lastIndex() + 1
                    self.currentTerm = max(self.currentTerm, self.ackEntries[server].term)
                    self.ackEntries[server].success = None
                    self.ackEntries[server].term = None

                if self.ackEntries[server].success == False:
                    self.nextIndex[server] -= 1
                    self.currentTerm = max(self.currentTerm, self.ackEntries[server].term)
                    self.ackEntries[server].success = None
                    self.ackEntries[server].term = None

            nLargestIndex = nlargest(math.ceil(len(self.peers)/2), self.matchIndex, key=self.matchIndex.get)
            nThLargestIndex = nLargestIndex[len(nLargestIndex)-1]
            if nThLargestIndex > self.commitIndex and self.log[nThLargestIndex].term == self.currentTerm:
                self.commitIndex = nThLargestIndex

    def global_update(self):
        self.all_server_update()
        self.followers_update()
        self.candidates_update()
        self.leaders_update()

    # CALL
    def requestVote(self, term, candidateId, lastLogIndex, lastLogTerm):
        if term < self.currentTerm:
            self.peers[candidateId].requestVoteAck(self.currentTerm, False, self.name)

        self.currentTerm = term

        if self.votedFor in (None, candidateId) and \
            lastLogIndex >= self.log.lastIndex():
            self.peers[candidateId].requestVoteAck(term, True, self.name)

        self.peers[candidateId].requestVoteAck(term, False, self.name)

    # CALL
    def requestVoteAck(self, term, success, senderID):
        self.ackElec[senderID].term = term
        self.ackElec[senderID].success = success

    # CALL
    def appendEntries(self, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
        if term < self.currentTerm:
            self.peers[leaderId].appendEntriesAck(self.currentTerm, False, self.log.lastIndex(), self.name)

        if entries == None:
            print("The RPC request received was a empty heartbeat")
            self.peers[leaderId].appendEntriesAck(term, True, self.log.lastIndex(), self.name)

        if prevLogIndex > self.log.__len__() or (prevLogIndex != 0 and self.log[prevLogIndex].term != prevLogTerm):
            self.peers[leaderId].appendEntriesAck(term, False, self.log.lastIndex(), self.name)

        #C'est censé déjà être le cas et si ça ne l'est pas, c'est que l'appel est mal formaté
        #entries = ServerLog(entries)

        i = prevLogIndex
        if prevLogIndex == 0:
            i += 1
        else:
            for ndx, new_log_entry in enumerate(entries):
                if self.log[i].term != entries[ndx].term:
                    self.log.clear_from(ndx)
                    # ndx is growing => if we clear from here there
                    # is nothing left to delete from self.log
                    break
                i += 1

        for ndx, new_log_entry in enumerate(entries):
            if ndx >= i - prevLogIndex:
                self.log.append_entry(entries[ndx])

        if leaderCommit > self.commitIndex:
            self.commitIndex = min(leaderCommit, self.log.lastIndex())

        if self.state == "Candidate":
            self.stepDown = True

        self.currentTerm = term
        self.peers[leaderId].appendEntriesAck(term, True, self.log.lastIndex(), self.name)

    # CALL
    def appendEntriesAck(self, term, success, lastIndex, senderID):
        self.ackEntries[senderID].term = term
        self.ackEntries[senderID].success = success
        self.ackEntries[senderID].lastIndex = lastIndex

if __name__ == '__main__':
    server1 = Server("Serv1")
    server2 = Server("Serv2")

    #pas encore testé et crash possible mais deverait pas
    i = 0
    while True:
        if not i % 10:
            server1.global_update()
            server2.global_update()
            print(i)
        i += 1
