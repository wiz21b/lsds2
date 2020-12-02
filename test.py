import json
from server import LogEntry, ServerLog, Server, ServerEncoder

le = LogEntry("cmd","term")
print(json.dumps(le, cls=ServerEncoder))

slog = ServerLog([le,le])
print(json.dumps(slog, cls=ServerEncoder))

d = json.loads(json.dumps(slog, cls=ServerEncoder))

ServerLog([LogEntry(j["command"],j["term"]) for j in d])

server = Server("one")


assert not server.appendEntries(term=1, leaderId="zulu1", prevLogIndex=2, prevLogTerm=3, entries=[], leaderCommit=1)
