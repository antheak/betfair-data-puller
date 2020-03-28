#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mar 18 2020

@author: tristanfulchiron
"""

import logging
import os
import logstash

HOST = 'logstash'

if os.path.exists("../data/data_puller_main.log"):
    os.remove("../data/data_puller_main.log")

logging.basicConfig(filename="../data/data_puller_main.log",
                    format='%(asctime)s %(levelname)s %(message)s',
                    level = 20)
logger = logging.getLogger('betfair_data_logger')
logger.addHandler(logstash.TCPLogstashHandler(HOST, 5000, version=1))