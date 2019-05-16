import mysql.connector as mariadb
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer

class dsmr5_server:
    def __init__(self, hostname='192.168.0.10', port=10080):
        self.logger = logging.getLogger('DSMR server')

        # self.logger.setLevel(logging.DEBUG)
        self.logger.setLevel(logging.INFO)

        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.info("Httpd server initialized")

        self.hostname = hostname
        self.port = port
        self.httpd = HTTPServer((hostname, port), DSMRHandler)

    def start(self):
        self.logger.info("Started")
        try:
            self.httpd.serve_forever()
        except KeyboardInterrupt:
            pass


class DSMRHandler(BaseHTTPRequestHandler):
    def __init__(self, request, client_address, server):
        BaseHTTPRequestHandler.__init__(self, request, client_address, server)

    def do_GET(self):
        path_list = self.path.split('/')
        print(path_list)
        if len(path_list) > 2:
            pass
        else:
            file = path_list[1]
            name, extension = file.rsplit('.', 1)
            if extension == 'html':
                try:
                    file_location = 'html/{0}'.format(file)
                    print(file_location)
                    f = open(file_location, "r")
                    content = f.read()
                    self.send_response(200)
                    self.send_header('Content-type', 'text/html')
                    self.end_headers()
                    self.wfile.write(bytes(content, "utf8"))
                except:
                    self.send_response(404)
            elif extension == 'css':
                pass
            elif extension == 'js':
                pass
            else:
                self.send_response(404)






"""
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

HOST_NAME = 'localhost'
PORT_NUMBER = 9000


class MyHandler(BaseHTTPRequestHandler):
    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        paths = {
            '/foo': {'status': 200},
            '/bar': {'status': 302},
            '/baz': {'status': 404},
            '/qux': {'status': 500}
        }

        if self.path in paths:
            self.respond(paths[self.path])
        else:
            self.respond({'status': 500})

    def handle_http(self, status_code, path):
        self.send_response(status_code)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        content = '''
        <html><head><title>Title goes here.</title></head>
        <body><p>This is a test.</p>
        <p>You accessed path: {}</p>
        </body></html>
        '''.format(path)
        return bytes(content, 'UTF-8')

    def respond(self, opts):
        response = self.handle_http(opts['status'], self.path)
        self.wfile.write(response)

if __name__ == '__main__':
    server_class = HTTPServer
    httpd = server_class((HOST_NAME, PORT_NUMBER), MyHandler)
    print(time.asctime(), 'Server Starts - %s:%s' % (HOST_NAME, PORT_NUMBER))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    print(time.asctime(), 'Server Stops - %s:%s' % (HOST_NAME, PORT_NUMBER))
"""
