import serial
import threading
import logging
import datetime
from collections import deque
import mysql.connector as mariadb


class DSMR(threading.Thread):
    debug = False

    def __init__(self):
        threading.Thread.__init__(self)
        self.version = 'v0.1'
        self.logger = logging.getLogger('DSMR')

        # self.logger.setLevel(logging.DEBUG)
        self.logger.setLevel(logging.INFO)

        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.info("DSMR v5.0.2 interpreter started")

        self.db = mariadb.connect(host='192.168.0.10', user='dsmr_user', passwd='dsmr_5098034ph', database='dsmr5')

        self.dsmr_queue = deque([])
        self.dsmr_reader = DSMRReader(self.dsmr_queue)
        self.dsmr_reader.start()

        self.last_voltage_L1 = 0
        self.last_voltage_L2 = 0
        self.last_voltage_L3 = 0
        self.get_last_values()

    def run(self):
        while True:
            try:
                if len(self.dsmr_queue):
                    next_line = self.dsmr_queue.popleft()
                    self.decode_line(next_line)
            except Exception as e:
                self.logger.error("Somthing went wrong getting line from queue")
                self.logger.error(e)

    def decode_line(self, next_line):
        """
        Split the line into OBISref and data. Selected OBIS references will be handled.
        :param next_line: Complete line from serial connection.
        :return: Nothing
        """
        next_line = next_line.decode('utf-8').strip()
        self.logger.debug(next_line)

        if len(next_line) < 8:
            self.logger.debug('No data in line')
        elif next_line[0] == '/':
            self.logger.debug('Start of Telegram')
        elif next_line[0] == '!':
            self.logger.debug('End of Telegram')
        else:
            try:
                OBISref, data = next_line.split('(', 1)
                data = data[:-1]

                if data.find('*'):
                    data = data[:data.find('*')]

                OBISref = OBISref.strip()
                data = data.strip()

                if OBISref == '1-0:32.7.0':
                    self.save_voltage_L1(data)
                elif OBISref == '1-0:52.7.0':
                    self.save_voltage_L2(data)
                elif OBISref == '1-0:72.7.0':
                    self.save_voltage_L3(data)
                else:
                    self.save_data(OBISref, data)
            except Exception as e:
                self.logger.error("Splitting line failed")
                self.logger.error(next_line)
                self.logger.error(e)

    def save_voltage_L1(self, data):
        self.logger.debug("save_voltage_L1: New voltage")
        if data != self.last_voltage_L1:
            cursor = self.db.cursor()
            sql = "INSERT INTO voltage_L1 (`date`, `value`) VALUES (%s, %s)"
            val = (datetime.datetime.now(), data)
            cursor.execute(sql, val)
            self.db.commit()
            self.last_voltage_L1 = data
        else:
            self.logger.debug("save_voltage_L1: Voltage hasn't changed")

    def save_voltage_L2(self, data):
        self.logger.debug("save_voltage_L2: New voltage")
        if data != self.last_voltage_L2:
            cursor = self.db.cursor()
            sql = "INSERT INTO voltage_L2 (`date`, `value`) VALUES (%s, %s)"
            val = (datetime.datetime.now(), data)
            cursor.execute(sql, val)
            self.db.commit()
            self.last_voltage_L2 = data
        else:
            self.logger.debug("save_voltage_L2: Voltage hasn't changed")

    def save_voltage_L3(self, data):
        self.logger.debug("save_voltage_L3: New voltage")
        if data != self.last_voltage_L3:
            cursor = self.db.cursor()
            sql = "INSERT INTO voltage_L3 (`date`, `value`) VALUES (%s, %s)"
            val = (datetime.datetime.now(), data)
            cursor.execute(sql, val)
            self.db.commit()
            self.last_voltage_L3 = data
        else:
            self.logger.debug("save_voltage_L3: Voltage hasn't changed")

    def save_data(self, OBISref, data):
        """
        If the OBISref is not yet implemented, Save all data to the data table.
        :param OBISref: OBIS reference from DSMR table
        :param data: value of OBIS. INT or FLOAT
        :return: Noting
        """
        self.logger.debug(OBISref, data)

        try:
            sql = "INSERT INTO data (OBIS_ref, value) VALUES (%s, %s)"
            val = (OBISref, data)

            cursor = self.db.cursor()
            cursor.execute(sql, val)
            self.db.commit()
            self.logger.debug("Data {0} inserted at {1}".format(data, cursor.lastrowid))
        except ValueError as e:
            self.logger.error("ValueError")
            self.logger.error("OBISred      : {0}".format(OBISref))
            self.logger.error("Data         : {0}".format(data))
            self.logger.error(e)
        except mariadb.errors.IntegrityError as e:
            self.logger.error("DB error")
            self.logger.error("OBISred      : {0}".format(OBISref))
            self.logger.error("Data         : {0}".format(data))
            self.logger.error(e)
        except:
            self.logger.error("Something went wrong!!!")
            self.logger.error("OBISred      : {0}".format(OBISref))
            self.logger.error("Data         : {0}".format(data))

    def get_last_values(self):
        cursor = self.db.cursor()

        # Get last voltage L1
        sql = "SELECT value FROM voltage_L1 ORDER BY date DESC LIMIT 1"
        cursor.execute(sql)
        results = cursor.fetchall()
        self.last_voltage_L1 = results[0][0]

        # Get last voltage L2
        sql = "SELECT value FROM voltage_L2 ORDER BY date DESC LIMIT 1"
        cursor.execute(sql)
        results = cursor.fetchall()
        self.last_voltage_L2 = results[0][0]

        # Get last voltage L3
        sql = "SELECT value FROM voltage_L3 ORDER BY date DESC LIMIT 1"
        cursor.execute(sql)
        results = cursor.fetchall()
        self.last_voltage_L3 = results[0][0]

class DSMRReader(threading.Thread):

    def __init__(self, output_queue=deque([])):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger('DSMR Reader')

        # self.logger.setLevel(logging.DEBUG)
        self.logger.setLevel(logging.INFO)

        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.info("thread initialised")

        self.output_queue = output_queue
        self.dsmr_serial = serial.Serial('/dev/ttyUSB0', 115200, parity=serial.PARITY_NONE)
        self.running = True

    def run(self):
        self.logger.info("thread started")

        while self.running:
            try:
                newline = self.dsmr_serial.readline()
                self.output_queue.append(newline)
            except:
                self.logger.error("cannot read line from serial")

    def stop(self):
        self.running = False
        self.dsmr_serial.close()


if __name__ == '__main__':
    dsmr = DSMR()
    dsmr.start()
