import serial
from dsmr5 import DSMR

ser = serial.Serial('/dev/ttyUSB0', 115200, parity=serial.PARITY_NONE)
running = 1
dsmr = DSMR(debug=True)
while running:
    line = ser.readline()
    dsmr.decode_line(line)