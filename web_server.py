import json
from flask import Flask, request
from server import LogEntry, ServerLog, Server, ServerEncoder

server = Server("one")

app = Flask(__name__)
@app.route("/appendEntries", methods=('PUT',))
def appendEntriesWeb():
    if request.method == 'PUT':
        return json.dumps(server.log, cls=ServerEncoder)
        return "zuliu" + request.form['term']



# from http.server  import  HTTPServer, BaseHTTPRequestHandler

# class WebHandler(BaseHTTPRequestHandler):
#     def do_GET(self):
#         self.wfile.write( "load".encode("utf-8"))

# def run(server_class=HTTPServer, handler_class=WebHandler):
#     server_address = ('', 8000)
#     httpd = server_class(server_address, handler_class)
#     httpd.serve_forever()


# run()
