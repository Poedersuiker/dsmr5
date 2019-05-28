import mysql.connector as mariadb
import json
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
            self.send_response(404)
        else:
            file = path_list[1]
            name, extension = file.rsplit('.', 1)
            if extension == 'html':
                try:
                    file_location = 'html/{0}'.format(file)
                    f = open(file_location, "r")
                    content = f.read()
                    self.send_response(200)
                    self.send_header('Content-type', 'text/html')
                    self.end_headers()
                    self.wfile.write(bytes(content, "utf8"))
                except Exception as e:
                    self.send_response(404)
                    self.wfile.write(bytes(e, "utf8"))
            elif extension == 'css':
                try:
                    file_location = 'css/{0}'.format(file)
                    f = open(file_location, "r")
                    content = f.read()
                    self.send_response(200)
                    self.send_header('Content-type', 'text/css')
                    self.end_headers()
                    self.wfile.write(bytes(content, "utf8"))
                except Exception as e:
                    self.send_response(404)
                    self.wfile.write(bytes(e, "utf8"))
            elif extension == 'js':
                try:
                    file_location = 'js/{0}'.format(file)
                    f = open(file_location, "r")
                    content = f.read()
                    self.send_response(200)
                    self.send_header('Content-type', 'text/javascript')
                    self.end_headers()
                    self.wfile.write(bytes(content, "utf8"))
                except Exception as e:
                    self.send_response(404)
                    self.wfile.write(bytes(e, "utf8"))
            elif extension in ('data', 'json'):
                try:
                    content = self.get_data(name)
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(bytes(content, "utf8"))
                except Exception as e:
                    self.send_response(404)
                    self.wfile.write(bytes(e, "utf8"))
            else:
                self.send_response(404)

    def get_data(self, name):
        json_content = json.dumps(name)
        if name == 'powerL1PP':
            db = mariadb.connect(host='192.168.0.10', user='dsmr_user', passwd='dsmr_5098034ph', database='dsmr5')
            cursor = db.cursor()
            sql = "SELECT date, value FROM power_L1_PP ORDER BY date DESC"
            cursor.execute(sql)
            results = cursor.fetchall()
            json_content = json.dumps(results, indent=4, sort_keys=True, default=str)
        elif name == 'powerL1MP':
            db = mariadb.connect(host='192.168.0.10', user='dsmr_user', passwd='dsmr_5098034ph', database='dsmr5')
            cursor = db.cursor()
            sql = "SELECT date, value FROM power_L1_MP ORDER BY date DESC"
            cursor.execute(sql)
            results = cursor.fetchall()
            json_content = json.dumps(results, indent=4, sort_keys=True, default=str)
        elif name == 'powerL2PP':
            db = mariadb.connect(host='192.168.0.10', user='dsmr_user', passwd='dsmr_5098034ph', database='dsmr5')
            cursor = db.cursor()
            sql = "SELECT date, value FROM power_L2_PP ORDER BY date DESC"
            cursor.execute(sql)
            results = cursor.fetchall()
            json_content = json.dumps(results, indent=4, sort_keys=True, default=str)
        elif name == 'powerL2MP':
            db = mariadb.connect(host='192.168.0.10', user='dsmr_user', passwd='dsmr_5098034ph', database='dsmr5')
            cursor = db.cursor()
            sql = "SELECT date, value FROM power_L2_MP ORDER BY date DESC"
            cursor.execute(sql)
            results = cursor.fetchall()
            json_content = json.dumps(results, indent=4, sort_keys=True, default=str)
        elif name == 'powerL3PP':
            db = mariadb.connect(host='192.168.0.10', user='dsmr_user', passwd='dsmr_5098034ph', database='dsmr5')
            cursor = db.cursor()
            sql = "SELECT date, value FROM power_L3_PP ORDER BY date DESC"
            cursor.execute(sql)
            results = cursor.fetchall()
            json_content = json.dumps(results, indent=4, sort_keys=True, default=str)
        elif name == 'powerL3MP':
            db = mariadb.connect(host='192.168.0.10', user='dsmr_user', passwd='dsmr_5098034ph', database='dsmr5')
            cursor = db.cursor()
            sql = "SELECT date, value FROM power_L3_MP ORDER BY date DESC"
            cursor.execute(sql)
            results = cursor.fetchall()
            json_content = json.dumps(results, indent=4, sort_keys=True, default=str)
        elif name == 'powerL1PP_week':
            db = mariadb.connect(host='192.168.0.10', user='dsmr_user', passwd='dsmr_5098034ph', database='dsmr5')
            cursor = db.cursor()
            sql = "SELECT date, value FROM power_L1_PP WHERE date > CURRENT_DATE() - INTERVAL 1 WEEK ORDER BY date DESC"
            cursor.execute(sql)
            results = cursor.fetchall()
            json_content = json.dumps(results, indent=4, sort_keys=True, default=str)
        elif name == 'powerL1MP_week':
            db = mariadb.connect(host='192.168.0.10', user='dsmr_user', passwd='dsmr_5098034ph', database='dsmr5')
            cursor = db.cursor()
            sql = "SELECT date, value FROM power_L1_MP WHERE date > CURRENT_DATE() - INTERVAL 1 WEEK ORDER BY date DESC"
            cursor.execute(sql)
            results = cursor.fetchall()
            json_content = json.dumps(results, indent=4, sort_keys=True, default=str)
        elif name == 'powerL2PP_week':
            db = mariadb.connect(host='192.168.0.10', user='dsmr_user', passwd='dsmr_5098034ph', database='dsmr5')
            cursor = db.cursor()
            sql = "SELECT date, value FROM power_L2_PP WHERE date > CURRENT_DATE() - INTERVAL 1 WEEK ORDER BY date DESC"
            cursor.execute(sql)
            results = cursor.fetchall()
            json_content = json.dumps(results, indent=4, sort_keys=True, default=str)
        elif name == 'powerL2MP_week':
            db = mariadb.connect(host='192.168.0.10', user='dsmr_user', passwd='dsmr_5098034ph', database='dsmr5')
            cursor = db.cursor()
            sql = "SELECT date, value FROM power_L2_MP WHERE date > CURRENT_DATE() - INTERVAL 1 WEEK ORDER BY date DESC"
            cursor.execute(sql)
            results = cursor.fetchall()
            json_content = json.dumps(results, indent=4, sort_keys=True, default=str)
        elif name == 'powerL3PP_week':
            db = mariadb.connect(host='192.168.0.10', user='dsmr_user', passwd='dsmr_5098034ph', database='dsmr5')
            cursor = db.cursor()
            sql = "SELECT date, value FROM power_L3_PP WHERE date > CURRENT_DATE() - INTERVAL 1 WEEK ORDER BY date DESC"
            cursor.execute(sql)
            results = cursor.fetchall()
            json_content = json.dumps(results, indent=4, sort_keys=True, default=str)
        elif name == 'powerL3MP_week':
            db = mariadb.connect(host='192.168.0.10', user='dsmr_user', passwd='dsmr_5098034ph', database='dsmr5')
            cursor = db.cursor()
            sql = "SELECT date, value FROM power_L3_MP WHERE date > CURRENT_DATE() - INTERVAL 1 WEEK ORDER BY date DESC"
            cursor.execute(sql)
            results = cursor.fetchall()
            json_content = json.dumps(results, indent=4, sort_keys=True, default=str)
        return json_content




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
