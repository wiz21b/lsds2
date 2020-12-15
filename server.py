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
        self.data = self.data[0:ndx-1]

    def has_entry( self, entry):
        for e in self._data:
            if e.term == entry.term and e.command == entry.command:
                return True
        return False

    def append_entry( self, entry):
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
        self.log = ServerLog()

        self.commitIndex = 0
        self.lastApplied = 0

        self.reset_leader_state()


    def reset_leader_state(self):
        self.nextIndex = dict()
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
            lastApplied += 1

            self.apply_state_machine(self.log[lastApplied])


    def requestVote(self, term, candidateId, lastLogIndex, lastLogTerm):
        if term < self.current_term:
            return False, False

        if votedFor in (None, candidateId) and \
           lastLogIndex >= self.log.lastIndex() and \
           lastLogTerm >= self.log.lastTerm():
            return self.currentTerm, True

    def appendEntries(self, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
        if term < self.currentTerm:
            return False

        if prevLogIndex >= len(self.log) or self.log[prevLogIndex].term != prevLogTerm:
            return False

        entries = ServerLog(entries)

        for ndx, new_log_entry in enumerate(entries):
            if self.log[ndx].term != entries[ndx].term:
                self.log.clear_from(ndx)
                # ndx is growing => if we clear from here there
                # is nothing left to delete from self.log
                break

        for new_log_entry in entries:
            if not self.log.has_entry( new_log_entry):
                self.log.append_entry(new_log_entry)

        if leaderCommit > self.commitIndex:
            self.commitIndex = min(leaderCommit, self.log.lastIndex())


if __name__ == '__main__':
    server = Server("blabla")
    server.persist_state()
