import serial
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

class DSMR:
    def __init__(self):
        self.version = 'v4.0'

    def decode_line(self, line):
        line = line.decode('utf-8').strip()
        logging.debug(line)
        if len(line) < 5:
            logging.warn('No data in line')
        elif line[0] == '/':
            logging.info('Start of Telegram')
        elif line[0] == '!':
            logging.info('End of Telegram')
        else:
            self.interpret_data(line)

    def interpret_data(self, line):
        logging.debug(line)
        OBISref, data = line.split('(', 1)
        logging.info(OBISref)

        if OBISref == '1-0:1.7.0':
            self.actual_electricity_power_delivered(data)

    def actual_electricity_power_delivered(self, data):
        logging.info(data[:-1])


logging.info('Start reading energymeter')
ser = serial.Serial('/dev/ttyUSB0', 115200, parity=serial.PARITY_NONE)
running = 1
dsmr = DSMR()
while running:
    line = ser.readline()
    print(line)
