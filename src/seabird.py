from queue import Queue
from threading import Thread
import threading
import pandas as pd
import numpy as np
import xml.etree.cElementTree as ET
import os
import sys
import serial
import time
import datetime

class SBE9Plus():
    def __init__(self):

        # Responsible for managing serial communication
        self.ser = serial.Serial()
        self.isComm = False

        # Lists with raw data received from serial
        self.raw_data = []
        
        # Used to verify that raw data is the right size
        self.raw_data_len = 0
        
        # Responsible for store the words removed from the raw data recived
        self.removed_words = []

        # Responsible for managing the exchange of data between threads
        self.queue = Queue()

        # Lists with sensor data
        self.temperature = []
        self.pressure = []
        self.conductivity = []
        self.salinity = []
        self.oxygen = []
        self.chlorophyll = []
        self.phycoerythrin = []
        self.cdom = []
        self.turbidity = []

        # Pressure offset, to be calculated on the surface before profiling
        self.pressure_offset = 0

        # Dictionaries with sensor calibration information
        self.temperature_cal = {}
        self.pressure_temperature_compensation = {}
        self.conductivity_cal = {}
        self.pressure_cal = {}
        self.altimeter_cal = {}
        self.oxygen_cal = {}
        self.chlorophyll_cal = {}
        self.phycoerythrin_cal = {}
        self.turbidity_cal = {}
        self.cdom_cal = {}
        self.calibration_coefficients = {}
        self.configuration_data = {}
        self.sensors = {}

        self.analog0_cal = {}
        self.analog1_cal = {}
        self.analog2_cal = {}
        self.analog3_cal = {}
        self.analog4_cal = {}
        self.analog5_cal = {}

        self.temperature_cal["Description"] = "Primary temperature"
        self.temperature_cal["Word"] = 0

        self.conductivity_cal["Description"] = "Primary conductivity"
        self.conductivity_cal["Word"] = 1

        self.pressure_cal["Description"] = "Pressure"
        self.pressure_cal["Word"] = 2

        self.analog0_cal["Description"] = "Analog Input 0"
        self.analog0_cal["Word"] = 5

        self.analog1_cal["Description"] = "Analog Input 1"
        self.analog1_cal["Word"] = 5

        self.analog2_cal["Description"] = "Analog Input 2"
        self.analog2_cal["Word"] = 6

        self.analog3_cal["Description"] = "Analog Input 3"
        self.analog3_cal["Word"] = 6

        self.analog4_cal["Description"] = "Analog Input 4"
        self.analog4_cal["Word"] = 7

        self.analog5_cal["Description"] = "Analog Input 5"
        self.analog5_cal["Word"] = 7

        self.pressure_temperature_compensation["Description"] = "Pressure sensor temperature"
        self.pressure_temperature_compensation["Word"] = 10

        # Lists with the raw data of the auxiliary inputs
        self.analog0 = []
        self.analog1 = []
        self.analog2 = []
        self.analog3 = []
        self.analog4 = []
        self.analog5 = []

        # DataFrame with data from all sensors
        self.scan = 0
        self.data = pd.DataFrame(columns=["Scan", "Time", "Temperature[°C]", "Pressure[dbar]", "Condutivity[S/m]",
                                          "Salinity[PSU]", "Analog Input 0[V]", "Analog Input 1[V]",
                                          "Analog Input 2[V]", "Analog Input 3[V]", "Analog Input 4[V]",
                                          "Analog Input 5[V]", "Project", "Station"])

        # Responsible for registering the profiling station
        self.station = "not informed"

        # Responsible for registering the project
        self.project = "not informed"

    def init_comm(self, port="/dev/ttyUSB0", baudrate=19200):
        """Starts communication with SBE11 through the serial port and creates a thread to buff received data.
        
        Keyword arguments:
        port -- Serial port address. (default "/dev/ttyUSB0")
        baudrate -- SBE11's baudrate (default 19200)        
        """
        try:
            self.ser.port = port
            self.ser.baudrate = baudrate
            self.ser.open()
            self.isComm = True
            self.t_stop = threading.Event()
            self.t = Thread(target=self.read_thread, args=(self.ser, self.queue, self.t_stop))
            self.t.setDaemon(True)
            self.t.start()
            return True
        except serial.SerialException:
            print("Error communicating with device!")
            print("Make sure that port and baudrate are correct!")
            print("Port = " + port)
            print("Baudrate = " + str(baudrate))
            return False

    def close_comm(self):
        if self.ser.isOpen():
            self.ser.write("S\n".encode())
            self.t_stop.set()
            self.ser.close()
            self.isComm = False

    def send_cmd(self, cmd):
        """Sends a string through the serial port.
        
        Keyword arguments:
        cmd -- String to be sent.
        """
        if self.ser.isOpen():
            self.ser.write(cmd.encode())
            time.sleep(0.25)

    def read_thread(self, ser, q, stop_event):
        while (not stop_event.is_set()):
            time.sleep(0.001)
            try:
                if ser.isOpen():
                    q.put([str(ser.readline()), datetime.datetime.now()])
            except serial.SerialException as e:
                print(e)
            except TypeError as e:
                print(e)

    def update(self):
        while self.queue.qsize() > 0:
            data = self.queue.get()
            self.convert_data_output(data[0], data[1])

    def get_data(self):
        self.update()
        return self.data.copy()

    def get_lastData(self):
        i = len(self.data)
        self.get_data()
        f = len(self.data)
        return self.data[i:f].copy().reset_index(drop=True)

    def start_collecting(self, n_avg=24):
        """Sends parameters required for SBE11 to start collecting SBE9 data.
        
        Keyword arguments:
        n_avg -- Number of scans that the SBE11 will calculate simple avarage, before sending 
        through the serial port. The SB9 has a sample rate of 24 Hz. (default 24)        
        """
        self.send_cmd("A" + str(n_avg) + "\n")
        self.send_cmd("U\n")
        for word in self.removed_words:
            self.send_cmd("X"+str(word)+"\n")
        self.send_cmd("GR\n")

    def stop_collecting(self):
        self.send_cmd("S\n")

    def get_dictionary(self, et):
        saida = {}
        for p in et:
            if "Coefficients" in p.tag:
                saida.update(self.get_dictionary(p))
            elif "SerialNumber" in p.tag or "CalibrationDate" in p.tag:
                saida[p.tag] = p.text
            else:
                saida[p.tag] = float(p.text)
        return saida

    def read_cal(self, cal_file, removed_words=[]):
        self.removed_words = removed_words
        cal = ET.ElementTree(file=cal_file)
        self.temperature_cal = self.get_dictionary(cal.iterfind(
            "Instrument/SensorArray/Sensor/TemperatureSensor/"))
        self.conductivity_cal = self.get_dictionary(cal.iterfind(
            "Instrument/SensorArray/Sensor/ConductivitySensor/"))
        self.pressure_cal = self.get_dictionary(cal.iterfind(
            "Instrument/SensorArray/Sensor/PressureSensor/"))
        self.altimeter_cal = self.get_dictionary(cal.iterfind(
            "Instrument/SensorArray/Sensor/AltimeterSensor/"))
        self.oxygen_cal = self.get_dictionary(cal.iterfind(
            "Instrument/SensorArray/Sensor/OxygenSensor/"))
        self.analog0_cal = {}
        self.analog1_cal = {}
        self.analog2_cal = {}
        self.analog3_cal = {}
        self.analog4_cal = {}
        self.analog5_cal = {}

        current_byte = 0
        if 0 not in removed_words:
            self.temperature_cal["First Byte Position"] = current_byte
            current_byte += 6

        if 1 not in removed_words:
            self.conductivity_cal["First Byte Position"] = current_byte
            current_byte += 6

        if 2 not in removed_words:
            self.pressure_cal["First Byte Position"] = current_byte
            current_byte += 6

        if 3 not in removed_words:
            current_byte += 6

        if 4 not in removed_words:
            current_byte += 6

        if 5 not in removed_words:
            self.analog0_cal["First Byte Position"] = current_byte
            current_byte += 3

        if 5 not in removed_words:
            self.analog1_cal["First Byte Position"] = current_byte
            current_byte += 3

        if 6 not in removed_words:
            self.analog2_cal["First Byte Position"] = current_byte
            current_byte += 3

        if 6 not in removed_words:
            self.analog3_cal["First Byte Position"] = current_byte
            current_byte += 3

        if 7 not in removed_words:
            self.analog4_cal["First Byte Position"] = current_byte
            current_byte += 3

        if 7 not in removed_words:
            self.analog5_cal["First Byte Position"] = current_byte
            current_byte += 3

        if 8 not in removed_words:
            current_byte += 6

        if 9 not in removed_words:
            current_byte += 6

        if 10 not in removed_words:
            self.pressure_temperature_compensation["First Byte Position"] = current_byte
            
        self.raw_data_len = current_byte + 6

    def convert_data_output(self, raw_data, time):
        raw_data = raw_data.replace("b'","").replace("\\r\\n'","")
        
        if len(raw_data) == self.raw_data_len:
            # primary temperature page 66 from manual 11pV2_017HighRes.pdf
            fbp = self.temperature_cal["First Byte Position"]
            tbyte0 = int(raw_data[fbp:fbp + 2], 16)
            tbyte1 = int(raw_data[fbp + 2:fbp + 4], 16)
            tbyte2 = int(raw_data[fbp + 4:fbp + 6], 16)

            tf = tbyte0 * 256 + tbyte1 + tbyte2 / 256

            tf0 = self.temperature_cal['F0']
            tg = self.temperature_cal["G"]
            th = self.temperature_cal["H"]
            ti = self.temperature_cal["I"]
            tj = self.temperature_cal["J"]

            its90 = 1 / (
                tg + th * np.log(tf0 / tf) + ti * (np.log(tf0 / tf)) ** 2 + tj * (np.log(tf0 / tf)) ** 3) - 273.15
            self.temperature.append(its90)

            fbp = self.pressure_cal["First Byte Position"]
            pbyte0 = int(raw_data[fbp:fbp + 2], 16)
            pbyte1 = int(raw_data[fbp + 2:fbp + 4], 16)
            pbyte2 = int(raw_data[fbp + 4:fbp + 6], 16)

            # Pressure Temperature Compensation page 68
            pM = self.pressure_cal['AD590M']
            pB = self.pressure_cal['AD590B']

            fbp = self.pressure_temperature_compensation["First Byte Position"]
            ptc = int(raw_data[fbp:fbp + 3], 16) * pM + pB

            pf = pbyte0 * 256 + pbyte1 + pbyte2 / 256

            pc1 = self.pressure_cal['C1']
            pc2 = self.pressure_cal['C2']
            pc3 = self.pressure_cal['C3']

            pc = pc1 + pc2 * ptc + pc3 * ptc ** 2

            pd1 = self.pressure_cal['D1']
            pd2 = self.pressure_cal['D2']

            pd = pd1 + pd2 * ptc

            pt1 = self.pressure_cal['T1']
            pt2 = self.pressure_cal['T2']
            pt3 = self.pressure_cal['T3']
            pt4 = self.pressure_cal['T4']
            pt5 = self.pressure_cal['T5']

            pt0 = pt1 + pt2 * ptc + pt3 * ptc ** 2 + pt4 * ptc ** 3 + pt5 * ptc ** 4

            pt = 1e6 / pf

            pressure = (pc * (1 - (pt0 ** 2) / (pt ** 2)) * (
                1 - pd * (1 - (pt0 ** 2) / (pt ** 2)))) + self.pressure_offset
            self.pressure.append(pressure)

            fbp = self.conductivity_cal["First Byte Position"]
            cbyte0 = int(raw_data[fbp:fbp + 2], 16)
            cbyte1 = int(raw_data[fbp + 2:fbp + 4], 16)
            cbyte2 = int(raw_data[fbp + 4:fbp + 6], 16)

            cf = (cbyte0 * 256 + cbyte1 + cbyte2 / 256) / 1000

            cg = self.conductivity_cal['G']
            ch = self.conductivity_cal['H']
            ci = self.conductivity_cal['I']
            cj = self.conductivity_cal['J']

            cpcor = self.conductivity_cal['CPcor']
            ctcor = self.conductivity_cal['CTcor']

            condutivity = (cg + ch * cf ** 2 + ci * cf ** 3 + cj * cf ** 4) / (
                10 * (1 + ctcor * its90 + cpcor * pressure))
            self.conductivity.append(condutivity)

            salinity = self.get_salinity(condutivity, its90, pressure)
            self.salinity.append(salinity)

            fbp = self.analog0_cal["First Byte Position"]
            analog0 = 5 * (1 - (int(raw_data[fbp:fbp + 3], 16) / 4095))

            self.analog0.append(analog0)

            fbp = self.analog1_cal["First Byte Position"]
            analog1 = 5 * (1 - (int(raw_data[fbp:fbp + 3], 16) / 4095))

            self.analog1.append(analog1)

            fbp = self.analog2_cal["First Byte Position"]
            analog2 = 5 * (1 - (int(raw_data[fbp:fbp + 3], 16) / 4095))

            self.analog2.append(analog2)

            fbp = self.analog3_cal["First Byte Position"]
            analog3 = 5 * (1 - (int(raw_data[fbp:fbp + 3], 16) / 4095))

            self.analog3.append(analog3)

            fbp = self.analog4_cal["First Byte Position"]
            analog4 = 5 * (1 - (int(raw_data[fbp:fbp + 3], 16) / 4095))

            self.analog4.append(analog4)

            fbp = self.analog5_cal["First Byte Position"]
            analog5 = 5 * (1 - (int(raw_data[fbp:fbp + 3], 16) / 4095))

            self.analog5.append(analog5)

            data_converted = {"Scan": self.scan, "Time": time, "Temperature[°C]": its90,
                              "Pressure[dbar]": pressure, "Condutivity[S/m]": condutivity,
                              "Salinity[PSU]": salinity,
                              "Analog Input 0[V]": analog0, "Analog Input 1[V]": analog1,
                              "Analog Input 2[V]": analog2, "Analog Input 3[V]": analog3,
                              "Analog Input 4[V]": analog4, "Analog Input 5[V]": analog5,
                              "Project": self.project, "Station": self.station}

            self.data.loc[self.scan] = data_converted
            self.scan += 1

    def to_csv(self, csvFilePath):

        self.get_data()

        if self.last_output < self.data["Scan"].iloc[-1]:
            df = self.data[self.data["Scan"] > self.last_output]
            self.last_output = self.data["Scan"].iloc[-1]
        else:
            df = self.data

        if not os.path.isfile(csvFilePath):
            df.to_csv(csvFilePath, mode='a', index=False)
        else:
            df.to_csv(csvFilePath, mode='a', index=False, header=False)

    ###############################################################################################
    # Practical Salinity PSS-78
    # valid from 2 to 42 psu
    # Page 130 from CTD/19plus/Documentacao/Seasave_7.23.2.pdf
    def get_salinity(self, cond, temp, press):

        # Explanation here http://salinometry.com/pss-78/
        if cond <= 0:
            return 0

        k = 0.0162

        a0 = 0.0080
        a1 = -0.1692
        a2 = 25.3851
        a3 = 14.0941
        a4 = -7.0261
        a5 = 2.7081

        A1 = 2.070e-5
        A2 = -6.370e-10
        A3 = 3.989e-15

        b0 = 0.0005
        b1 = -0.0056
        b2 = -0.0066
        b3 = -0.0375
        b4 = 0.0636
        b5 = -0.0144

        B1 = 3.426e-2
        B2 = 4.464e-4
        B3 = 4.215e-1
        B4 = -3.107e-3

        c0 = 6.766097e-1
        c1 = 2.00564e-2
        c2 = 1.104259e-4
        c3 = -6.9698e-7
        c4 = 1.0031e-9

        R = cond / 4.2914

        T68 = 1.00024 * temp

        Rp = 1 + (A1 * press + A2 * press ** 2 + A3 * press ** 3) / (
            1 + B1 * T68 + B2 * T68 ** 2 + B3 * R + B4 * T68 * R)

        temp_coeff = c0 + c1 * T68 + c2 * T68 ** 2 + c3 * T68 ** 3 + c4 * T68 ** 4

        Rt = R / (temp_coeff * Rp)

        deltaS = ((T68 - 15) / (1 + k * (T68 - 15))) * (
            b0 + b1 * Rt ** 0.5 + b2 * Rt + b3 * Rt ** 1.5 + b4 * Rt ** 2 + b5 * Rt ** 2.5)

        S = a0 + a1 * Rt ** 0.5 + a2 * Rt + a3 * Rt ** 1.5 + \
            a4 * Rt ** 2 + a5 * Rt ** 2.5 + deltaS

        return S

    ###############################################################################################
    # Oxygen saturation with the method of Garcia and Gordom.
    # Valid for -5<T<50 and 0<S<60.
    # Page 136 from CTD/19plus/Documentacao/Seasave_7.23.2.pdf
    def get_oxsol(self, temp, sal):
        if temp > -5 and temp < 50 and sal > 0 and sal < 60:

            a0 = 2.00907
            a1 = 3.22014
            a2 = 4.0501
            a3 = 4.94457
            a4 = -0.256847
            a5 = 3.88767

            b0 = -0.00624523
            b1 = -0.00737614
            b2 = -0.010341
            b3 = -0.00817083

            c0 = -0.000000488682

            ts = np.log((298.15 - temp) / (273.15 - temp))

            oxsol = np.exp(a0 + a1 * ts + a2 * ts ** 2 + a3 * ts ** 3 + a4 * ts ** 4 + a5 * ts ** 5 + sal * (
                b0 + b1 * ts + b2 * ts ** 2 + b3 * ts ** 3) + c0 * sal ** 2)

            return oxsol
        else:
            return None

    def get_oxygen(self, V, T, S, P):
        """Oxygen concentration

        according to ./Documentacao/43-3431 Oxygen Calibration.pdf

        :param V = instrument output (volts):
        :param T = temperature (°C):
        :param S = salinity (PSU):
        :param P = pressure (dbar):
        :return oxygen concentration (ml/l):
        """
        # COEFFICIENTS:
        A = -3.4792e-003
        B = 1.7854e-004
        C = -2.5166e-006
        E = 0.036
        Soc = 0.4373
        Voffset = -0.5147
        Tau20 = 1.52

        # NOMINAL DYNAMIC COEFFICIENTS
        D1 = 1.92634e-4
        H1 = -3.300000e-2
        D2 = -4.64803e-2
        H2 = 5.00000e+3
        H3 = 1.45000e+3

        # Temperature in Kelvin
        K = T + 273.15

        return Soc * (V + Voffset) * (1.0 + A * T + B * T ** 2 + C * T ** 3) * self.get_oxsol(T, S) * np.exp(E * P / K)

    def get_conductivity(self, f, t, p):
        """
        Conductivity

        according to ./doc/04-4615 Conductivity Calibration.pdf

        :param f = Instrument Output (kHz):
        :param t = temperature (°C):
        :param p = pressure (decibars):
        :return conductivity (S/m):
        """

        g = self.conductivity_cal['G']
        h = self.conductivity_cal['H']
        i = self.conductivity_cal['I']
        j = self.conductivity_cal['J']

        CPcor = self.conductivity_cal['CPcor']

        CTcor = self.conductivity_cal['CTcor']

        return (g + h * f ** 2 + i * f ** 3 + j * f ** 4) / (
            10 * (1 + CTcor * t + CPcor * p))

    def get_pressure(self, U, T):
        """
        Pressure

        according to ./d/09-1291 Pressure Calibration.pdf

        :param U = temperature (deg C):
        :param T = pressure period (usec):
        :return Pressure (psia):
        """

        # PRESSURE COEFFICIENTS
        C1 = self.pressure_cal['C1']
        C2 = self.pressure_cal['C2']
        C3 = self.pressure_cal['C3']

        D1 = self.pressure_cal['D1']
        D2 = self.pressure_cal['D2']

        T1 = self.pressure_cal['T1']
        T2 = self.pressure_cal['T2']
        T3 = self.pressure_cal['T3']
        T4 = self.pressure_cal['T4']
        T5 = self.pressure_cal['T5']

        C = C1 + C2 * U + C3 * U ** 2

        D = D1 + D2 * U

        T0 = T1 + T2 * U + T3 * U ** 2 + T4 * U ** 3 + T5 * U ** 4

        return C * (1 - (T0 ** 2) / (T ** 2)) * (1 - D * (1 - (T0 ** 2) / (T ** 2)))


"""
###############################################################################
# Example
ctd = SBE9Plus()
try:
	ctd.init_comm()
	ctd.read_cal("./Cal/9Plus.xmlcon", [8])
	ctd.project = "Marine E-Tech"
	ctd.station = input("Digite o nome da estação!")
	ctd.start_collecting("24")
	ctd.send_to_ES = False
	while True:
		ctd.get_data()
		ctd.to_csv("./9plus.csv")
		print(ctd.get_lastData())
		time.sleep(1)
except KeyboardInterrupt:
	ctd.stop_collecting()
	ctd.close_comm()"""
