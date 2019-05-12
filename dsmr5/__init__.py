import serial
import logging
import mysql.connector as mariadb


class DSMR:
    debug = False

    def __init__(self):
        self.version = 'v0.1'
        self.logger = logging.getLogger('EnergyMeter')

        # self.logger.setLevel(logging.DEBUG)
        self.logger.setLevel(logging.INFO)

        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.info("DSMR v5.0.2 interpreter started")

        self.db = mariadb.connect(host='192.168.0.10', user='dsmr_user', passwd='dsmr_5098034ph', database='dsmr5')


    def decode_line(self, line):
        line = line.decode('utf-8').strip()
        self.logger.debug(line)
        if len(line) < 8:
            self.logger.debug('No data in line')
        elif line[0] == '/':
            self.logger.debug('Start of Telegram')
        elif line[0] == '!':
            self.logger.debug('End of Telegram')
        else:
            self.save_data(line)

    def save_data(self, line):
        self.logger.debug(line)
        OBISref = ""
        data = ""
        try:
            OBISref, data = line.split('(', 1)
            data = data[:-1]

            if data.find('*'):
                data = data[:data.find('*')]

            OBISref = OBISref.strip()
            data = data.strip()

            sql = "INSERT INTO data (OBIS_ref, value) VALUES (%s, %s)"
            val = (OBISref, data)

            cursor = self.db.cursor()
            cursor.execute(sql, val)
            self.db.commit()
            self.logger.debug("Data {0} inserted at {1}".format(data, cursor.lastrowid))
        except ValueError as e:
            self.logger.error("ValueError")
            self.logger.error("Input line   : {0}".format(line))
            self.logger.error("OBISred      : {0}".format(OBISref))
            self.logger.error("Data         : {0}".format(data))
            self.logger.error(e)
        except mariadb.errors.IntegrityError as e:
            self.logger.error("DB error")
            self.logger.error("Input line   : {0}".format(line))
            self.logger.error("OBISred      : {0}".format(OBISref))
            self.logger.error("Data         : {0}".format(data))
            self.logger.error(e)
        except:
            self.logger.error("Something went wrong!!!")
            self.logger.error("Input line   : {0}".format(line))
            self.logger.error("OBISred      : {0}".format(OBISref))
            self.logger.error("Data         : {0}".format(data))


if __name__ == '__main__':
    ser = serial.Serial('/dev/ttyUSB0', 115200, parity=serial.PARITY_NONE)
    running = 1
    dsmr = DSMR()
    while running:
        line = ser.readline()
        print(line)
