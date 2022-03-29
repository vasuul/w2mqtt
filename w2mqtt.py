#!/usr/bin/env python3

import os
import sys
import time
import json
import smbus
import signal
import paho.mqtt.client as mqtt

from os import path
from time import sleep
from queue import SimpleQueue
from ctypes import c_short
from threading import Thread
from subprocess import check_output


VERSION = "0.0.2"

class BME280:
    def __init__(self, addr, bus_num=1):
        self._addr = addr
        self._bus_num = bus_num
        self._bus = smbus.SMBus(self._bus_num)
        self.setup()

    def setup(self):
        # Write to the config register
        # 20ms standby, 16x filter cooef, disable SPI
        self._bus.write_byte_data(self._addr, 0xF5, 0xF0)

        # Then write the humidity control register
        self._bus.write_byte_data(self._addr, 0xF2, 0x00) # disable

        # Then we write to the control register
        # 16 temp/press oversampling - normal mode
        self._bus.write_byte_data(self._addr, 0xF4, 0xB3)

        # then get our cal data
        self.tcal = [
            self.read_ushort(0x88),
            self.read_short(0x8A),
            self.read_short(0x8C)]
        self.pcal = [
            self.read_ushort(0x8E),
            self.read_short(0x90),
            self.read_short(0x92),
            self.read_short(0x94),
            self.read_short(0x96),
            self.read_short(0x98),
            self.read_short(0x9A),
            self.read_short(0x9C),
            self.read_short(0x9E)]

    def reading(self):
        # Read temp and pressure
        rawd = self._bus.read_i2c_block_data(self._addr, 0xF7, 6)
        rawp = (rawd[0] << 12) | (rawd[1] << 4) | (rawd[2] >> 4)
        rawt = (rawd[3] << 12) | (rawd[4] << 4) | (rawd[5] >> 4)

        # Refine temperature - straight from the datasheet
        rt1 = ((((rawt >> 3) - (self.tcal[0]<<1)) * self.tcal[1]) >> 11)
        rt2 = ((((rawt >> 4) - self.tcal[0]) * ((rawt >> 4) - self.tcal[0]) >> 12) * self.tcal[2]) >> 14
        temp = (((rt1 + rt2) * 5) + 128)  >> 8

        # Refine pressure - straight from the datasheet
        rp1 = (rt1 + rt2) - 128000
        rp2 = rp1 * rp1 * self.pcal[5]
        rp2 = rp2 + ((rp1 * self.pcal[4]) << 17)
        rp2 = rp2 + (self.pcal[3] << 35)
        rp1 = ((rp1 * rp1 * self.pcal[2]) >> 8) + ((rp1 * self.pcal[1]) <<12)
        rp1 = (((1 << 47) + rp1) * self.pcal[0]) >> 33
        if rp1 == 0:
            pres = 0
        else:
            pres = 1048576 - rawp
            pres = int((((pres << 31) - rp2) * 3125 ) / rp1)
            rp1 = (self.pcal[8] * (pres >> 13) * (pres >> 13) ) >> 25
            rp2 = (self.pcal[7] * pres ) >> 19
            pres = ((pres + rp1 + rp2) >> 8) + (self.pcal[6] << 4)

        return temp/100.0, (pres/256.0)/100.0

    def read_short(self, reg):
        dat = self._bus.read_i2c_block_data(self._addr, reg, 2)
        return c_short((dat[1] << 8) | dat[0]).value

    def read_ushort(self, reg):
        dat = self._bus.read_i2c_block_data(self._addr, reg, 2)
        return (dat[1] << 8) | dat[0]

    def read_byte(self, reg):
        dat = self._bus.read_i2c_block_data(self._addr, reg, 1)
        if dat > 127:
            return dat - 256
        else:
            return dat

    def read_ubyte(self, reg):
        dat = self._bus.read_i2c_block_data(self._addr, reg, 1)
        return dat

    def publishMQTT(self, mqttc, baseTopic):
        t,p = self.reading()
        topic = "base"
        if len(baseTopic) > 0:
            topic = baseTopic + "/" + topic
        payload = "{ "
        payload += "\"temp\": {tt}, \"press\": {pp}".format(tt=t, pp=p)
        payload += " }"
        mqttc.publish(topic, payload)


class SensorList:
    def __init__(self, sensorNames):
        # Keep track of timestamp / id pairs.  Many of these sensors
        #  like to send things in tripilicate, and there isn't any reason
        #  to send that down the line
        self.lastMessage = {}
        self.sensors = {}
        self.sensorNames = sensorNames

    def process(self, data):

        # we need to know what this sensor is
        if not 'id' in data:
            return

        # check to see if this is a repeat message
        if ( 'id' in data and 'time' in data and
             data['id'] in self.lastMessage and
             data['time'] == self.lastMessage[data['id']]):
            # We already have a message from this sensor with this timestamp so let it go
            return
        else:
            # Keep track of this one
            self.lastMessage[data['id']] = data['time']
            
        if not data['id'] in self.sensors:
            # we haven't seen this sensor yet, so make a new one
            name = ""
            if data['id'] in self.sensorNames:
                name = self.sensorNames[data['id']]

            model = ""
            if 'model' in data:
                model = data['model']

            self.sensors[data['id']] = Sensor(model, data['id'], name)

        # Then we just process the data like normal    
        if self.sensors[data['id']].update(data):
            yield self.sensors[data['id']]
        
class Sensor:
    # Create a sensor reading with a given model and id
    def __init__(self, model, sID, name=""):
        self.model = model
        self.myID = sID
        self.name = name

        # basic measurement information all sensors have
        self.measure_time = ""
        self.temp = 0
        self.humidity = 0
        self.battery = True

        # some (but not all) sensors have a settable hannel (A,B,C)
        self.channel = 'Z'

        # Temperature probe
        self.ptemp = 0

        # Atlas only information
        self.wind_speed = []
        self.wind_dir = 0
        self.last_rain = -1
        self.rain = 0
        self.strikes = []
        self.strike_dist = []
        self.uv = 0
        self.lux = 0
        self.atlas_seen = {37:False, 38:False, 39:False}

        # Signal information
        self.rssi = 0
        self.noise = 0
        self.snr = 0
        
    def update(self, jsonData):
        # Make sure this really is for us
        if 'id' in jsonData and jsonData['id'] != self.myID:
            return False

        if 'channel' in jsonData:
            self.channel = jsonData['channel']

        if 'battery_ok' in jsonData:
            if jsonData['battery_ok'] == 1:
                self.battery = True
            else:
                self.battery = False
        if 'temperature_C' in jsonData:
            self.temp = jsonData['temperature_C']

        if 'temperature_F' in jsonData:
            self.temp = (jsonData['temperature_F'] - 32.0) * (5.0 / 9.0)

        if 'temperature_1_C' in jsonData:
            self.ptemp = jsonData['temperature_1_C']

        if 'temperature_1_F' in jsonData:
            self.ptemp = (jsonData['temperature_1_F'] - 32.0) * (5.0 / 9.0)

        if 'humidity' in jsonData:
            self.humidity = jsonData['humidity']

        if 'time' in jsonData:
            self.measure_time = jsonData['time']

        if 'rssi' in jsonData:
            self.rssi = jsonData['rssi']

        if 'snr' in jsonData:
            self.snr = jsonData['snr']

        if 'noise' in jsonData:
            self.noise = jsonData['noise']

        if 'wind_avg_mi_h' in jsonData:
            self.wind_speed.append(jsonData['wind_avg_mi_h'])

        if 'wind_avg_km_h' in jsonData:
            self.wind_speed.append(jsonData['wind_avg_km_h'] / 1.609344)

        if 'wind_dir_deg' in jsonData:
            self.wind_dir = jsonData['wind_dir_deg']

        if 'strike_count' in jsonData:
            self.strikes.append(jsonData['strike_count'])

        if 'strike_distance' in jsonData:
            self.strike_dist.append(jsonData['strike_distance'])
        
        if 'uv' in jsonData:
            self.uv = jsonData['uv']

        if 'lux' in jsonData:
            self.lux = jsonData['lux']

        # TODO: Look into what this reports and when
        if 'rain_in' in jsonData:
            if self.last_rain < 0:
                self.last_rain = jsonData['rain_in']
            self.rain = jsonData['rain_in']

        if 'message_type' in jsonData:
            mt = jsonData['message_type']
            if mt not in [37, 38, 39]:
                print(" ******** Strange message type: ")
                print(jsonData)
            else:
                self.atlas_seen[mt] = True

        # We need all three message type to get a full atlas reading, so wait until 
        #  we see all three
        if self.model == 'Acurite-Atlas':
            if self.atlas_seen[37] and self.atlas_seen[38] and self.atlas_seen[39]:
                return True
            else:
                return False

        # Other sensors are complete in one reading
        return True

    def topic(self):
        if self.name == "":
            return str(self.myID)
        else:
            return self.name

    def publishMQTTJSON(self, client, baseTopic = ""):
        tc = self.topic()
        if len(baseTopic) > 0:
            tc = baseTopic + "/" + tc

        pld = "{"
        pld += "\"temp\": {val}, ".format(val=self.temp)
            
        pld += "\"hum\": {val}, ".format(val=self.humidity)
            
        if self.battery:
            pld += "\"batt\": true, "
        else:
            pld += "\"batt\": false, "

        if self.model == "Acurite-Tower":
            # this also has a channel
            pld += "\"chan\": \"{val}\", ".format(val=self.channel)
        elif self.model == "Acurite-00275rm":
            # Temperature probes
            pld += "\"ptemp\": {val}, ".format(val=self.ptemp)
        elif self.model == "Acurite-Atlas":
            # Whole bunch of stuff to do here...
            pld += "\"chan\": \"{val}\", ".format(val=self.channel)
            pld += "\"uv\": {val}, ".format(val=self.uv)
            pld += "\"lux\": {val}, ".format(val=self.lux)
            pld += "\"wind_dir\": {val}, ".format(val=self.wind_dir)
            # wind speed, strike count, and strike distance are in every message,
            #  so we will take the average of them
            #if len(self.wind_speed) != 3 or len(self.strikes) != 3 or len(self.strike_dist) != 3:
            #    print(" ******** we have more elements than expected")
            #    print("{},{},{}".format(len(self.wind_speed), len(self.strikes), len(self.strike_dist)))
            pld += "\"wind_speed\": {val}, ".format(val=sum(self.wind_speed) / len(self.wind_speed))
            pld += "\"strikes\": {val}, ".format(val=sum(self.strikes) / len(self.strikes))
            pld += "\"strike_dist\": {val}, ".format(val=sum(self.strike_dist) / len(self.strike_dist))
            delta_rain = self.rain - self.last_rain
            if delta_rain < 0:
                delta_rain = 0
            pld += "\"rain_delta\": {val}, ".format(val=delta_rain)
            pld += "\"rain\": {val}, ".format(val=self.rain)
            self.last_rain = self.rain
            # Then reset all our cumulative atlas measurements
            self.wind_speed.clear()
            self.strikes.clear()
            self.strike_dist.clear()
            self.atlas_seen = {37:False, 38:False, 39:False}

        pld += "\"rssi\": {val}, ".format(val=self.rssi)
        pld += "\"noise\": {val}, ".format(val=self.noise)

        pld += "\"time\": \"{val}\"".format(val=self.measure_time)

        pld += "}"

        client.publish(tc, pld)

    def publishMQTTIndividual(self, client, baseTopic = ""):
        tc = self.topic()
        if len(baseTopic) > 0:
            tc = baseTopic + "/" + tc
            
        client.publish(tc + "/temp_c", self.temp)
        client.publish(tc + "/temp_f", round((self.temp * (9.0/5.0)) + 32, 1))
            
        client.publish(tc + "/humidity", self.humidity)
            
        if self.battery:
            client.publish(tc + "/battery", "good")
        else:
            client.publish(tc + "/battery", "bad")

        if self.model == "Acurite-00275rm":
            # Temperature probes
            client.publish(tc + "/ptemp_c", self.ptemp)
            client.publish(tc + "/ptemp_f", round((self.ptemp * (9.0/5.0)) + 32, 1))

def processInput(q):
    print("Processing input!!")
    while(True):
        try:
            line = sys.stdin.readline()
            q.put(line)
        except KeyboardInterrupt as e:
            print("Caught keyboard interrupt in readline")
            break
        
            
def main(configFile = "/etc/w2mqtt.conf"):
    mqtt_config = {}

    # We need to get the pid of the rtl process that is backing us
    
    try:
        sleep(1)
        pids = list(map(int, check_output(["pidof","rtl_433"]).split()))
        if len(pids) == 0:
            print("Cannot find the trtl_433 process")
            return -1
    except:
        print("Failed to run pid program")
        return -1

    print("Pid of program: {}", pids[0])
    
    # We have the option to give the sensors better names
    sensorNames = {}
    sensorDB_IDs = {}
    
    if path.exists(configFile):
        print("Loading config file: " + configFile)
        with open(configFile, 'r') as configFile:
            configData = configFile.read()
            config = json.loads(configData)

            if 'mqtt' in config:
                mqtt_config = config['mqtt']

            if 'rename' in config:
                for name in config['rename'].keys():
                    sid = config['rename'][name]
                    sensorNames[sid] = name
                print(sensorNames)
    else:
        print("Could not find config: " + configFile)


    # Connect to the mqtt server
    try:
        print(mqtt_config)
        mqttc = mqtt.Client()
        mqttc.username_pw_set(mqtt_config['username'], mqtt_config['password'])
        mqttc.connect(mqtt_config['server'], mqtt_config['port'])
        mqttc.loop_start()
    except:
        print("Could not open mqtt server")
        return -2

    # Create a queue to get data from the input processing thread
    inpq = SimpleQueue()
    worker = Thread(target=processInput, args=(inpq,))
    worker.daemon = True
    sensors = SensorList(sensorNames)
    pres = BME280(0x77)

    lastPressureTime = 0
    lastInputTime = time.time()
    ignored = []
    # And just sit here waiting for data
    worker.start()
    while True:
        thisTime = time.time()
        if not inpq.empty():
            line = inpq.get()
            data = json.loads(line)
            if not 'id' in data:
                continue
            if data['id'] in ignored:
                continue
            if not data['id'] in sensorNames:
                ignored.append(data['id'])
                mqttc.publish("unknown/weather/{}".format(data['id']))
                continue
            
            for sensor in sensors.process(data):
                sensor.publishMQTTJSON(mqttc, mqtt_config['topic'])
                lastInputTime = thisTime
        else:
            # Just wait a small bit of time for more data
            sleep(0.1)

        # Check to see if we should try and read from our barometer
        #  read from it every 30 seconds
        if thisTime - lastPressureTime > 15:
            # Read pressure and temp from bme280 sensor
            pres.publishMQTT(mqttc, mqtt_config['topic'])
            lastPressureTime = thisTime

        if thisTime - lastInputTime > (60):
            # RTL 433 seems to have locked up.  The only way I have 
            #  found to get things going again is to kill it
            #  SIGERM is not sufficient so send SIGKILL
            print(" **** rtl_433 seems to have frozen **** ")
            topic = "base/restart"
            if len(mqtt_config['topic']) > 0:
                topic = mqtt_config['topic'] + "/" + topic
            mqttc.publish(topic, 1)
            os.kill(pids[0], signal.SIGKILL)
            break
            
    # Disconnect from the mqtt broker
    mqttc.loop_stop()
    mqttc.disconnect()
        
if __name__ == '__main__':
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        main()
