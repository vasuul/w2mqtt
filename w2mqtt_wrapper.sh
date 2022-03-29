#!/bin/bash

/usr/local/bin/rtl_433 -M level -F json | /usr/bin/python3 /usr/local/bin/w2mqtt.py

exit 2
