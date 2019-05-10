import serial
import logging
from dsmr5 import DSMR

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

logging.info('Start reading energymeter')
ser = serial.Serial('/dev/ttyUSB0', 115200, parity=serial.PARITY_NONE)
running = 1
dsmr = DSMR()
while running:
    line = ser.readline()
    print(line)
    dsmr.decode_line(line)