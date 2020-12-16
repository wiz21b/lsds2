import json

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
    def __init__(self, name):
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


    def requestVote(self, term, candidateId, lastLogIndex, lastLogTerm):    #Potential issue regarding term vs lastLogTerm -> inconsitancy between paper and video from creator
        if term < self.currentTerm:
            return self.currentTerm, False

        if self.votedFor in (None, candidateId) and \
            lastLogIndex >= self.log.lastIndex():
            self.currentTerm = term
            return term, True

        return term, False

    def appendEntries(self, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
        if term < self.currentTerm:
            return self.currentTerm, False

        if entries == None:
            print("The RPC request received was a empty heartbeat")
            return term, True

        if prevLogIndex > self.log.__len__() or (prevLogIndex != 0 and self.log[prevLogIndex].term != prevLogTerm):
            return False, False

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

        self.currentTerm = term
        return term, True

if __name__ == '__main__':

    #----------------------------------------------------------------------------------#
    #                          Construction d'un exemple type                          #
    #                                         -                                        #
    #  Une étape manquante ne permettera pas un test a plus grande échelle sans crash  #
    #----------------------------------------------------------------------------------#

    #Pour la doc des RPC: https://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14

    server1 = Server("Serv1")
    server2 = Server("Serv2")

    #We have to add here commit index update (not mandatory for all current test, only requiered for final algo)

    #Simulating election request from server 1 (intiated by timeout as no leader currently exist)
    server1.currentTerm += 1
    server1.votedFor = "Serv1"
    
    #Simulating a RPC (election) from server1 to (here server2 only) all servers
    retTerm, retSuccess = server2.requestVote(server1.currentTerm, server1.name, server1.log.lastIndex(), server1.log.__getitem__(server1.log.lastIndex()).term)
    server2.currentTerm = retTerm #Mandatory to allow old leader that lost track for a while to step down and get updated back

    print("Return values: ")
    print("retTerm =", retTerm)
    print("retSuccess =", retSuccess)

    #Here a test should be implemented after the responses of the other servers to the candidate(s)
    #If a canditate has at least nbOfServers/2 True returns from the other servers.
    #If one does, it is the new leader and can talk to the client + should send heartbeat via appendEntries RPC

    #Here server1 is elected
    server1.votedFor = None

    #Implementing leader knowlege of other servers (at every ellection reset new leader knowledge like shown)
    server1.nextIndex["serverTwo"] = server1.log.lastIndex() + 1
    server1.matchIndex["serverTwo"] = 0

    #Leader send heartbeat RPC to all other servers to say he is the new leader
    retTerm, retSuccess = server2.appendEntries(server1.currentTerm, server1.name, None, None, None, server1.commitIndex)
    server2.currentTerm = retTerm

    if not retSuccess:
        print("Critical issue in empty heartbeat")
        assert retSuccess

    #Reset server2 random timeout (150 - 300ms OR >>> broadcastTime)
    
    #If a server is a candidate, it should now become a follower

    #Now leader waits for his small timeout --> send empty heartbeat / an user inputs --> broadcast new entry

    #Simulating an user input
    server1.log.append_entry(LogEntry("myBeatifullAction", 1)) #The 1 is determined by server 1 (because he is suppose to be the leader) and is his term

    #Reset leader heartbeat timeout

    #Leader verification of follower's logs update
    if server1.log.__len__() >= server1.nextIndex["serverTwo"]: 
        #Simulating reaction from leader --> RPC to followers
        entries = server1.log.getItemFrom(server1.nextIndex["serverTwo"]) 
        retTermServ2, retSuccessServ2 = server2.appendEntries(server1.currentTerm, server1.name, server1.nextIndex["serverTwo"]-1,
                                                                entries[0].term, entries, server1.commitIndex)
        server1.currentTerm = retTermServ2

        #Reset server2 random timeout (150 - 300ms OR >>> broadcastTime)

        print("Return values: ")
        print("retTermServ2 =", retTermServ2)
        print("retSuccessServ2 =", retSuccessServ2)
        
        #If return is False, then the next index for the server X stored in the leader is still too big, retry at next iteration with decremented index
        if not retSuccessServ2:
            server1.nextIndex["serverTwo"] -= 1

        #Server log verification
        tmp = ""
        for elem in server2.log._data:
            tmp += "("
            tmp += str(elem.command)
            tmp += ", "
            tmp += str(elem.term)
            tmp += ") "
        print(server2.name + " log: " + tmp)

        #We can restart to line 164
