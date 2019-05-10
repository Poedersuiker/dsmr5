import serial
import logging


class DSMR:
    def __init__(self):
        self.version = 'v0.1'
        self.logger = logging.getLogger('EnergyMeter')
        self.logger.setLevel(logging.DEBUG)

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.info("DSMR v5.0.2 interpreter started")

    def decode_line(self, line):
        line = line.decode('utf-8').strip()
        self.logger.debug(line)
        if len(line) < 5:
            self.logger.warning('No data in line')
        elif line[0] == '/':
            self.logger.info('Start of Telegram')
        elif line[0] == '!':
            self.logger.info('End of Telegram')
        else:
            self.interpret_data(line)

    def interpret_data(self, line):
        self.logger.debug(line)
        OBISref, data = line.split('(', 1)
        self.logger.info(OBISref)

        if OBISref == '1-0:1.7.0':
            self.actual_electricity_power_delivered(data)

    def actual_electricity_power_delivered(self, data):
        self.logger.info(data[:-1])


if __name__ == '__main__':
    ser = serial.Serial('/dev/ttyUSB0', 115200, parity=serial.PARITY_NONE)
    running = 1
    dsmr = DSMR()
    while running:
        line = ser.readline()
        print(line)
