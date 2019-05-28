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
        self.last_power_L1_PP = 0
        self.last_power_L1_MP = 0
        self.last_power_L2_PP = 0
        self.last_power_L2_MP = 0
        self.last_power_L3_PP = 0
        self.last_power_L3_MP = 0

        if datetime.datetime.now().minute < 15:
            self.power_delivered_tariff1_times = [15, 30, 45, 0]
            self.power_delivered_tariff2_times = [15, 30, 45, 0]
            self.power_supplied_tariff1_times = [15, 30, 45, 0]
            self.power_supplied_tariff2_times = [15, 30, 45, 0]
        elif datetime.datetime.now().minute < 30:
            self.power_delivered_tariff1_times = [30, 45, 0, 15]
            self.power_delivered_tariff2_times = [30, 45, 0, 15]
            self.power_supplied_tariff1_times = [30, 45, 0, 15]
            self.power_supplied_tariff2_times = [30, 45, 0, 15]
        elif datetime.datetime.now().minute < 45:
            self.power_delivered_tariff1_times = [45, 0, 15, 30]
            self.power_delivered_tariff2_times = [45, 0, 15, 30]
            self.power_supplied_tariff1_times = [45, 0, 15, 30]
            self.power_supplied_tariff2_times = [45, 0, 15, 30]
        else:
            self.power_delivered_tariff1_times = [0, 15, 30, 45]
            self.power_delivered_tariff2_times = [0, 15, 30, 45]
            self.power_supplied_tariff1_times = [0, 15, 30, 45]
            self.power_supplied_tariff2_times = [0, 15, 30, 45]

        self.last_power_delivered_tariff1 = 0
        self.last_power_delivered_tariff2 = 0
        self.last_power_supplied_tariff1 = 0
        self.last_power_supplied_tariff2 = 0

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

                # Voltages L1, L2 and L3
                if OBISref == '1-0:32.7.0':
                    self.save_voltage_L1(data)
                elif OBISref == '1-0:52.7.0':
                    self.save_voltage_L2(data)
                elif OBISref == '1-0:72.7.0':
                    self.save_voltage_L3(data)

                # Power L1 +P -P
                elif OBISref == '1-0:21.7.0':
                    self.save_power_L1_PP(data)
                elif OBISref == '1-0:22.7.0':
                    self.save_power_L1_MP(data)

                # Power L2 +P -P
                elif OBISref == '1-0:41.7.0':
                    self.save_power_L2_PP(data)
                elif OBISref == '1-0:42.7.0':
                    self.save_power_L2_MP(data)

                # Power L3 +P -P
                elif OBISref == '1-0:61.7.0':
                    self.save_power_L3_PP(data)
                elif OBISref == '1-0:62.7.0':
                    self.save_power_L3_MP(data)

                # Power Delivered to home
                elif OBISref == '1-0:1.8.1':
                    self.save_power_delivered_tariff1(data)
                elif OBISref == '1-0:1.8.2':
                    self.save_power_delivered_tariff2(data)

                # Power Supplied from solar system
                elif OBISref == '1-0:2.8.1':
                    self.save_power_supplied_tariff1(data)
                elif OBISref == '1-0:2.8.2':
                    self.save_power_supplied_tariff2(data)

                # Save all other data
                else:
                    self.save_data(OBISref, data)
            except Exception as e:
                self.logger.error("Splitting line failed")
                self.logger.error(next_line)
                self.logger.error(e)

    def save_power_L1_PP(self, data):
        self.logger.debug("save_power_L1_PP: New power")
        if data != self.last_power_L1_PP:
            cursor = self.db.cursor()
            sql = "INSERT INTO power_L1_PP (`date`, `value`) VALUES (%s, %s)"
            val = (datetime.datetime.now(), data)
            cursor.execute(sql, val)
            self.db.commit()
            self.last_power_L1_PP = data
        else:
            self.logger.debug("save_power_L1_PP: Power hasn't changed")

    def save_power_L1_MP(self, data):
        self.logger.debug("save_power_L1_MP: New power")
        if data != self.last_power_L1_MP:
            cursor = self.db.cursor()
            sql = "INSERT INTO power_L1_MP (`date`, `value`) VALUES (%s, %s)"
            val = (datetime.datetime.now(), data)
            cursor.execute(sql, val)
            self.db.commit()
            self.last_power_L1_MP = data
        else:
            self.logger.debug("save_power_L1_MP: Power hasn't changed")

    def save_power_L2_PP(self, data):
        self.logger.debug("save_power_L2_PP: New power")
        if data != self.last_power_L2_PP:
            cursor = self.db.cursor()
            sql = "INSERT INTO power_L2_PP (`date`, `value`) VALUES (%s, %s)"
            val = (datetime.datetime.now(), data)
            cursor.execute(sql, val)
            self.db.commit()
            self.last_power_L2_PP = data
        else:
            self.logger.debug("save_power_L2_PP: Power hasn't changed")

    def save_power_L2_MP(self, data):
        self.logger.debug("save_power_L2_MP: New power")
        if data != self.last_power_L2_MP:
            cursor = self.db.cursor()
            sql = "INSERT INTO power_L2_MP (`date`, `value`) VALUES (%s, %s)"
            val = (datetime.datetime.now(), data)
            cursor.execute(sql, val)
            self.db.commit()
            self.last_power_L2_MP = data
        else:
            self.logger.debug("save_power_L2_MP: Power hasn't changed")

    def save_power_L3_PP(self, data):
        self.logger.debug("save_power_L3_PP: New power")
        if data != self.last_power_L3_PP:
            cursor = self.db.cursor()
            sql = "INSERT INTO power_L3_PP (`date`, `value`) VALUES (%s, %s)"
            val = (datetime.datetime.now(), data)
            cursor.execute(sql, val)
            self.db.commit()
            self.last_power_L3_PP = data
        else:
            self.logger.debug("save_power_L3_PP: Power hasn't changed")

    def save_power_L3_MP(self, data):
        self.logger.debug("save_power_L3_MP: New power")
        if data != self.last_power_L3_MP:
            cursor = self.db.cursor()
            sql = "INSERT INTO power_L3_MP (`date`, `value`) VALUES (%s, %s)"
            val = (datetime.datetime.now(), data)
            cursor.execute(sql, val)
            self.db.commit()
            self.last_power_L3_MP = data
        else:
            self.logger.debug("save_power_L3_MP: Power hasn't changed")

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

    def save_power_delivered_tariff1(self, data):
        self.logger.debug("save_power_deliverd_tariff1: Next row")
        if datetime.datetime.now().minute == self.power_delivered_tariff1_times[0]:
            delta = data - self.last_power_delivered_tariff1
            cursor = self.db.cursor()
            sql = "INSERT INTO power_delivered_tariff1 (`date`, `meter`, `delta`) VALUES (%s, %s, %s)"
            val = (datetime.datetime.now(), data, delta)
            cursor.execute(sql, val)
            self.db.commit()
            self.last_power_delivered_tariff1 = data
            self.next_power_delivered_tariff1()
        else:
            self.logger.debug("save_power_delivered_tariff1: Not time yet")

    def save_power_delivered_tariff2(self, data):
        self.logger.debug("save_power_delivered_tariff2: Next row")
        if datetime.datetime.now().minute == self.power_delivered_tariff2_times[0]:
            delta = data - self.last_power_delivered_tariff2
            cursor = self.db.cursor()
            sql = "INSERT INTO power_delivered_tariff2 (`date`, `meter`, `delta`) VALUES (%s, %s, %s)"
            val = (datetime.datetime.now(), data, delta)
            cursor.execute(sql, val)
            self.db.commit()
            self.last_power_delivered_tariff2 = data
            self.next_power_delivered_tariff2()
        else:
            self.logger.debug("save_power_delivered_tariff2: Not time yet")

    def save_power_supplied_tariff1(self, data):
        self.logger.debug("save_power_supplied_tariff1: Next row")
        if datetime.datetime.now().minute == self.power_supplied_tariff1_times[0]:
            delta = data - self.last_power_supplied_tariff1
            cursor = self.db.cursor()
            sql = "INSERT INTO power_supplied_tariff1 (`date`, `meter`, `delta`) VALUES (%s, %s, %s)"
            val = (datetime.datetime.now(), data, delta)
            cursor.execute(sql, val)
            self.db.commit()
            self.last_power_supplied_tariff1 = data
            self.next_power_supplied_tariff1()
        else:
            self.logger.debug("save_power_supplied_tariff1: Not time yet")

    def save_power_supplied_tariff2(self, data):
        self.logger.debug("save_power_supplied_tariff2: Next row")
        if datetime.datetime.now().minute == self.power_supplied_tariff2_times[0]:
            delta = data - self.last_power_supplied_tariff2
            cursor = self.db.cursor()
            sql = "INSERT INTO power_supplied_tariff2 (`date`, `meter`, `delta`) VALUES (%s, %s, %s)"
            val = (datetime.datetime.now(), data, delta)
            cursor.execute(sql, val)
            self.db.commit()
            self.last_power_supplied_tariff2 = data
            self.next_power_supplied_tariff2()
        else:
            self.logger.debug("save_power_supplied_tariff2: Not time yet")

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
            self.logger.error("OBISref      : {0}".format(OBISref))
            self.logger.error("Data         : {0}".format(data))
            self.logger.error(e)
        except mariadb.errors.IntegrityError as e:
            self.logger.error("DB error")
            self.logger.error("OBISref      : {0}".format(OBISref))
            self.logger.error("Data         : {0}".format(data))
            self.logger.error(e)
        except:
            self.logger.error("Something went wrong!!!")
            self.logger.error("OBISref      : {0}".format(OBISref))
            self.logger.error("Data         : {0}".format(data))

    def next_power_delivered_tariff1(self):
        self.logger.info("power_delivered_tariff1 filed {0}".format(self.power_delivered_tariff1_times[0]))
        new_list = self.power_delivered_tariff1_times[1:] + self.power_delivered_tariff1_times[:1]
        self.power_delivered_tariff1_times = new_list
        self.logger.info("power_delivered_tariff1 next {0}".format(self.power_delivered_tariff1_times[0]))

    def next_power_delivered_tariff2(self):
        self.logger.info("power_delivered_tariff2 filed {0}".format(self.power_delivered_tariff2_times[0]))
        new_list = self.power_delivered_tariff2_times[1:] + self.power_delivered_tariff2_times[:1]
        self.power_delivered_tariff2_times = new_list
        self.logger.info("power_delivered_tariff2 next {0}".format(self.power_delivered_tariff2_times[0]))

    def next_power_supplied_tariff1(self):
        self.logger.info("power_supplied_tariff1 filed {0}".format(self.power_supplied_tariff1_times[0]))
        new_list = self.power_supplied_tariff1_times[1:] + self.power_supplied_tariff1_times[:1]
        self.power_supplied_tariff1_times = new_list
        self.logger.info("power_supplied_tariff1 next {0}".format(self.power_supplied_tariff1_times[0]))

    def next_power_supplied_tariff2(self):
        self.logger.info("power_supplied_tariff2 filed {0}".format(self.power_supplied_tariff2_times[0]))
        new_list = self.power_supplied_tariff2_times[1:] + self.power_supplied_tariff2_times[:1]
        self.power_supplied_tariff2_times = new_list
        self.logger.info("power_supplied_tariff2 next {0}".format(self.power_supplied_tariff2_times[0]))

    def get_last_values(self):
        cursor = self.db.cursor()

        # Get last voltage L1
        try:
            sql = "SELECT value FROM voltage_L1 ORDER BY date DESC LIMIT 1"
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) > 0:
                self.last_voltage_L1 = results[0][0]
            else:
                self.last_voltage_L1 = 0

            # Get last voltage L2
            sql = "SELECT value FROM voltage_L2 ORDER BY date DESC LIMIT 1"
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) > 0:
                self.last_voltage_L2 = results[0][0]
            else:
                self.last_voltage_L2 = 0

            # Get last voltage L3
            sql = "SELECT value FROM voltage_L3 ORDER BY date DESC LIMIT 1"
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) > 0:
                self.last_voltage_L3 = results[0][0]
            else:
                self.last_voltage_L3 = 0

            # Get last power L1 +P
            sql = "SELECT value FROM power_L1_PP ORDER BY date DESC LIMIT 1"
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) > 0:
                self.last_power_L1_PP = results[0][0]
            else:
                self.last_power_L1_PP = 0

            # Get last power L1 -P
            sql = "SELECT value FROM power_L1_MP ORDER BY date DESC LIMIT 1"
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) > 0:
                self.last_power_L1_MP = results[0][0]
            else:
                self.last_power_L1_MP = 0

            # Get last power L2 +P
            sql = "SELECT value FROM power_L2_PP ORDER BY date DESC LIMIT 1"
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) > 0:
                self.last_power_L2_PP = results[0][0]
            else:
                self.last_power_L2_PP = 0

            # Get last power L2 -P
            sql = "SELECT value FROM power_L2_MP ORDER BY date DESC LIMIT 1"
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) > 0:
                self.last_power_L2_MP = results[0][0]
            else:
                self.last_power_L2_MP = 0

            # Get last power L3 +P
            sql = "SELECT value FROM power_L3_PP ORDER BY date DESC LIMIT 1"
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) > 0:
                self.last_power_L3_PP = results[0][0]
            else:
                self.last_power_L3_PP = 0

            # Get last power L3 -P
            sql = "SELECT value FROM power_L3_MP ORDER BY date DESC LIMIT 1"
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) > 0:
                self.last_power_L3_MP = results[0][0]
            else:
                self.last_power_L3_MP = 0

            # Get last power delivered tariff1
            sql = "SELECT meter FROM power_delivered_tariff1 ORDER BY date DESC LIMIT 1"
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) > 0:
                self.last_power_delivered_tariff1 = results[0][0]
            else:
                self.last_power_delivered_tariff1 = 0

            # Get last power delivered tariff2
            sql = "SELECT meter FROM power_delivered_tariff2 ORDER BY date DESC LIMIT 1"
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) > 0:
                self.last_power_delivered_tariff2 = results[0][0]
            else:
                self.last_power_delivered_tariff2 = 0

            # Get last power supplied tariff1
            sql = "SELECT meter FROM power_supplied_tariff1 ORDER BY date DESC LIMIT 1"
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) > 0:
                self.last_power_supplied_tariff1 = results[0][0]
            else:
                self.last_power_supplied_tariff1 = 0

            # Get last power supplied tariff2
            sql = "SELECT meter FROM power_supplied_tariff2 ORDER BY date DESC LIMIT 1"
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) > 0:
                self.last_power_supplied_tariff2 = results[0][0]
            else:
                self.last_power_supplied_tariff2 = 0
        except:
            self.logger.error("Last values couldn't be retreived")

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
