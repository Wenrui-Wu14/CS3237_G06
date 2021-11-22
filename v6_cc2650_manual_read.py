# -*- coding: utf-8 -*-
"""
TI CC2650 SensorTag
-------------------

Adapted by Ashwin from the following sources:
 - https://github.com/IanHarvey/bluepy/blob/a7f5db1a31dba50f77454e036b5ee05c3b7e2d6e/bluepy/sensortag.py
 - https://github.com/hbldh/bleak/blob/develop/examples/sensortag.py

"""
import asyncio
import datetime
from os import walk
import platform
import struct
import time

from bleak import BleakClient

import csv
import datetime as dt 

import paho.mqtt.client as mqtt
import json

import enum

result_main = [] # store the data readings from all the required sensors
NUM_DATA = 1000 # number of data readings to collect (this is not the sampling rate)
system_addr = "F0:F8:F2:86:3A:81" # change this to ur system id


MOVE_PERIOD = bytearray([0x0A]) # 10 Hz
ENV_PERIOD = bytearray([0x64]) # 1 Hz

#run: 10 Hz => resolution x hex value = 10ms x 0x0A = 100ms = 0.1s = 10Hz
#walk: 5 Hz => resolution x hex value = 10ms x 0x14 = 200ms = 0.2s = 5Hz
#rest: 1 Hz => resolution x hex value = 10ms x 0x64 = 1000ms = 1s = 1Hz
buzzerFlag = False

class Service:
    """
    Here is a good documentation about the concepts in ble;
    https://learn.adafruit.com/introduction-to-bluetooth-low-energy/gatt

    In TI SensorTag there is a control characteristic and a data characteristic which define a service or sensor
    like the Light Sensor, Humidity Sensor etc

    Please take a look at the official TI user guide as well at
    https://processors.wiki.ti.com/index.php/CC2650_SensorTag_User's_Guide
    """

    def __init__(self):
        self.count = 0
        self.walk_count = 0 # changes made
        self.run_count = 0 # changes made
        self.data_uuid = None
        self.ctrl_uuid = None
        self.period_uuid = None

    async def read(self, client):
        raise NotImplementedError()

class Sensor(Service):

    def callback(self, sender: int, data: bytearray):
        raise NotImplementedError()

    async def enable(self, client, *args):
        # start the sensor on the device
        write_value = bytearray([0x01])
        await client.write_gatt_char(self.ctrl_uuid, write_value)
        write_value = bytearray([0x0A]) # check the sensor period applicable values in the sensor tag guide mentioned above
        await client.write_gatt_char(self.period_uuid, write_value)

        return self

    async def read(self, client):
        val = await client.read_gatt_char(self.data_uuid)
        return self.callback(1, val)

# ==================================================================
class MovementSensorMPU9250SubService:

    def __init__(self):
        self.bits = 0

    def enable_bits(self):
        return self.bits

    def callback_sensor(self, data):
        raise NotImplementedError()


class MovementSensorMPU9250(Sensor):
    GYRO_XYZ = 7 # 111
    ACCEL_XYZ = 7 << 3 # 111000 = 100111000
    MAG_XYZ = 1 << 6 # 1000000
    ACCEL_RANGE_2G  = 0 << 8
    ACCEL_RANGE_4G  = 1 << 8 # 100000000
    ACCEL_RANGE_8G  = 2 << 8
    ACCEL_RANGE_16G = 3 << 8

    def __init__(self):
        super().__init__()
        self.data_uuid = "f000aa81-0451-4000-b000-000000000000"
        self.ctrl_uuid = "f000aa82-0451-4000-b000-000000000000"
        self.period_uuid = "f000aa83-0451-4000-b000-000000000000"
        self.ctrlBits = 0

        self.sub_callbacks = []

    def register(self, cls_obj: MovementSensorMPU9250SubService):
        self.ctrlBits |= cls_obj.enable_bits()
        self.sub_callbacks.append(cls_obj.callback_sensor)

    async def enable(self, client, *args):
        # start the sensor on the device
        await client.write_gatt_char(self.ctrl_uuid, struct.pack("<H", self.ctrlBits))

        # # listen using the handler
        # await client.start_notify(self.data_uuid, self.callback)

        # await client.write_gatt_char(self.period_uuid, struct.pack("<H", self.ctrlBits))
        await client.write_gatt_char(self.period_uuid, MOVE_PERIOD) # 1 Hz

        return self

    def callback(self, sender: int, data: bytearray):
        result_merged = []
        unpacked_data = struct.unpack("<hhhhhhhhh", data)
        # unpack the data from result returned from AccelerometerSensorMovementSensorMPU9250 callback_sensor
        # unpack the data from result returned from MagnetometerSensorMovementSensorMPU9250 callback_sensor
        # unpack the data from result returned from GyroscopeSensorMovementSensorMPU9250 callback_sensor
        for cb in self.sub_callbacks: 
            # append the unpacked data to array and return the array for 
            # reading using the defined read in Sensor(Service) class
            result_merged.append(cb(unpacked_data)) 
        return result_merged


class AccelerometerSensorMovementSensorMPU9250(MovementSensorMPU9250SubService):
    def __init__(self):
        super().__init__()
        self.bits = MovementSensorMPU9250.ACCEL_XYZ | MovementSensorMPU9250.ACCEL_RANGE_4G
        self.scale = 8.0/32768.0 # TODO: why not 4.0, as documented? @Ashwin Need to verify

    def callback_sensor(self, data):
        '''Returns (x_accel, y_accel, z_accel) in units of g'''
        rawVals = data[3:6]

        # print("[MovementSensor] Accelerometer:", tuple([ v*self.scale for v in rawVals ]))
        # if never return will just print the result, and the defined callback function in MovementSensorMPU9250 will print the result
        return tuple([ v*self.scale for v in rawVals ])
# ==================================================================

class HumiditySensor(Sensor):
    def __init__(self):
        super().__init__()
        self.count = 0 # this serves as the packet number
        self.data_uuid = "f000aa21-0451-4000-b000-000000000000"
        self.ctrl_uuid = "f000aa22-0451-4000-b000-000000000000"
        self.period_uuid = "f000aa23-0451-4000-b000-000000000000"

    async def enable(self, client, *args):
        # start the sensor on the device
        write_value = bytearray([0x01])
        await client.write_gatt_char(self.ctrl_uuid, write_value)
        # set the period for sensor
        await client.write_gatt_char(self.period_uuid, ENV_PERIOD) # 0.5 Hz
        return self

    def callback(self, sender: int, data: bytearray):
        self.count += 1 
        (rawT, rawH) = struct.unpack('<HH', data)
        temp = -40.0 + 165.0 * (rawT / 65536.0)
        RH = 100.0 * (rawH / 65536.0)
        return self.count, temp, RH


class BatteryService(Service):
    def __init__(self):
        super().__init__()
        self.data_uuid = "00002a19-0000-1000-8000-00805f9b34fb"

    async def read(self, client):
        val = await client.read_gatt_char(self.data_uuid)
        return int(val[0])

class LEDAndBuzzer(Service):
    """
        Adapted from various sources. Src: https://evothings.com/forum/viewtopic.php?t=1514 and the original TI spec
        from https://processors.wiki.ti.com/index.php/CC2650_SensorTag_User's_Guide#Activating_IO
        Codes:
            1 = red
            2 = green
            3 = red + green
            4 = buzzer
            5 = red + buzzer
            6 = green + buzzer
            7 = all
    """
    def __init__(self):
        super().__init__()
        self.data_uuid = "f000aa65-0451-4000-b000-000000000000" #1010,1010,0110,0101 = 0xaa01
        self.ctrl_uuid = "f000aa66-0451-4000-b000-000000000000" #1010,1010,0110,0110

    async def enable(self, client):
        # enable the config
        write_value = bytearray([0x01]) 
        await client.write_gatt_char(self.data_uuid, bytearray([0x00]))  # sensortag IO service data field will be 0x7F right after connected(due to the self test result). So what you should do is before changing it to remote mode, write a 0 to the data field and the change to remote.
        await client.write_gatt_char(self.ctrl_uuid, write_value) # enable In remote mode the BLE host overrides the IO usage and can activate the LEDs and the buzzer directly.
        return self

    async def notify(self, client, code):
        write_value = bytearray([0x01])
        await client.write_gatt_char(self.ctrl_uuid, write_value)
        # turn on buzzer as stated from the list above using 0x04
        write_value = bytearray([code])
        await client.write_gatt_char(self.data_uuid, write_value)

async def run(address, mqttclient):
    async with BleakClient(address) as client:
        x = await client.is_connected()
        print("Connected: {0}".format(x))

        led_and_buzzer = await LEDAndBuzzer().enable(client)
        humidity_sensor = await HumiditySensor().enable(client)
        acc_sensor = AccelerometerSensorMovementSensorMPU9250()

        movement_sensor = MovementSensorMPU9250()
        movement_sensor.register(acc_sensor)

        move_sensor = await movement_sensor.enable(client)

        battery = BatteryService()

        prev_batter_reading_time = time.time()
        batter_reading = await battery.read(client)
        print("Battery Reading", batter_reading)

        while True:
            # set according to your period in the sensors; otherwise sensor will return same value for all the readings
            # till the sensor refreshes as defined in the period
            print("this is buzzer flag in run:{}---------------\n".format(buzzerFlag))
            if buzzerFlag:
                #print("buzz buzz buzz")
                await led_and_buzzer.notify(client, 0x04)
                # HOW to make it buzz longer?
            elif not buzzerFlag:
                await led_and_buzzer.notify(client, 0x00)
                print("no buzzybuz")
            await asyncio.sleep(0.08)  # slightly less than 100ms to accommodate time to print results
            # data = await asyncio.gather(light_sensor.read(client), humidity_sensor.read(client), move_sensor.read(client))
            data = await asyncio.gather(humidity_sensor.read(client), move_sensor.read(client))
            result_main.append([data[0][1], data[0][2], data[1][0][0], data[1][0][1], data[1][0][2]])
            print("--------------------------------------------")
            print("This is the data")
            print(data)
            data_packet = {
                "ID": datetime.datetime.now().strftime('%Y%m%d%H%M%S'),
                "temperature": data[0][1],
                "humidity": data[0][2],
                "acc_data": data[1][0]
            }
            send_data_packet(mqttclient, data_packet)
            # print("Index {}:\nTemp: {}\nRelative Humidity: {}\nAccelerometer_X: {}\nAccelerometer_Y: {}\nAccelerometer_Z: {}".format(data[0][0], data[0][1], data[0][2], data[1][0][0], data[1][0][1], data[1][0][2]))
            print("--------------------------------------------")
            if data[0][0] == NUM_DATA: # change this value deemed necessary
                print("--------------------------------------------Writing to csv file--------------------------------------")
                write_to_csv(result_main)

            if time.time() - prev_batter_reading_time > 1*60:  # 15 mins
                batter_reading = await battery.read(client)
                print("Battery Reading", batter_reading)
                prev_batter_reading_time = time.time()

def write_to_csv(data):
    # writing to csv file based on the date and time of execution, year, month, day, hour, minutes, seconds
    outputfilename = 'output_{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
    # still not sure how to diffentiate the device 
    header = ['ambient_temp', 'relative_humidity', 'x_accel', 'y_accel', 'z_accel']

    # writing to csv file 
    with open(outputfilename, 'w', newline='') as csvfile: 
        # creating a csv writer object 
        csvwriter = csv.writer(csvfile) 
            
        # writing the fields 
        csvwriter.writerow(header) 
            
        # writing the data rows 
        csvwriter.writerows(data)

# MQTT functions
def on_connect(client, userdata, flags, rc):
    if rc==0:
        print("Connected successfully.")
        client.subscribe("Group_06/Proj/warning")
    else:
        print("Failed to connect. Error code: %d." % rc)

def on_message(client, userdata, msg):
    global buzzerFlag

    print("Received message from ML server.")
    # return prediction result
    resp_dict = json.loads(msg.payload)
    if resp_dict["warning"] == 1:
        buzzerFlag = True
    elif resp_dict["warning"] == 0:
        buzzerFlag = False

    print("this is buzzer flag in on message:{}---------------\n".format(buzzerFlag))
    print("ID: %s, Acc_prediction: %s, Warning: %d, Risk score: %f, HI: %f"
% (resp_dict["ID"], resp_dict["acc_prediction"], resp_dict["warning"], resp_dict["risk_score"], resp_dict["HI"]))

def setup(hostname):
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(hostname)
    client.loop_start()
    return client

def send_data_packet(client, data_packet):
    print("sending data_packet to ML server ...")
    print(data_packet)
    client.publish("Group_06/Proj/classify", json.dumps(data_packet))

if __name__ == "__main__":
    """
    To find the address, once your sensor tag is blinking the green led after pressing the button, run the discover.py
    file which was provided as an example from bleak to identify the sensor tag device
    """
    
    # MQTT setup
    mqttclient = setup("127.0.0.1") # change this

    import os

    os.environ["PYTHONASYNCIODEBUG"] = str(1)
    address = (
        system_addr
        if platform.system() != "Darwin"
        else "6FFBA6AE-0802-4D92-B1CD-041BE4B4FEB9"
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(address, mqttclient))
    loop.run_forever()
