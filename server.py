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
        assert 1 <= ndx <= len(self._data)
        return self._data[ndx-1]

    def clear_from( self, ndx):
        assert 1 <= ndx <= len(self._data)
        self.data = self.data[0:ndx-1] #C'est pas un clear to ndx et pas from ndx là ?

    def has_entry( self, entry):
        for e in self._data:
            if e.term == entry.term and e.command == entry.command:
                return True
        return False

    def append_entry( self, entry):
        assert isinstance(entry, LogEntry)
        self._data.append(entry)

    def lastIndex( self):
        return len(self._data) + 1

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
        self.log = ServerLog() # An array [1..]

        # self.log[1] = LogEntry(action, term)
        # self.log[45].term .action

        self.commitIndex = 0
        self.lastApplied = 0

        self.reset_leader_state()

    def reset_leader_state(self):
        self.nextIndex = dict()

        # self.nextIndex[ peer.name ] = 3

        self.matchIndex = 0

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


    def requestVote(self, term, candidateId, lastLogIndex, lastLogTerm):
        if term < self.currentTerm:
            return self.currentTerm, False

        if self.votedFor in (None, candidateId) and \
            lastLogIndex >= self.log.lastIndex():
            self.currentTerm = term
            return term, True

        return False, False

    def appendEntries(self, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
        if term < self.currentTerm:
            return self.currentTerm, False

        if prevLogIndex >= len(self.log) or self.log[prevLogIndex].term != prevLogTerm:
            return False, False

        #C'est censé déjà être le cas et si ça ne l'est pas, c'est que l'appel est mal formaté
        #entries = ServerLog(entries) 

        i = prevLogIndex
        for ndx, new_log_entry in enumerate(entries):
            if self.log[i].term != entries[ndx].term:
                self.log.clear_from(ndx)
                # ndx is growing => if we clear from here there
                # is nothing left to delete from self.log
                break
            i += 1

        for ndx, new_log_entry in enumerate(entries):
            if ndx >= i - prevLogIndex:
                self.log.append_entry(new_log_entry)

        if leaderCommit > self.commitIndex:
            self.commitIndex = min(leaderCommit, self.log.lastIndex())

        self.currentTerm = term
        return term, True

if __name__ == '__main__':
    server = Server("blabla")
    server.persist_state()
