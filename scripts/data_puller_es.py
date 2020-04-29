#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mar 23 2020

@author: tristanfulchiron
"""

from es_pandas import es_pandas
from data_puller_logger import logger
from time import sleep
import credentials


es_host = 'elasticsearch:9200'
WAIT_TIME = 30

# Not very elegant, but this is v0 is resilient, and allows the betfair container to wait for the elastic one to be
# set up before launching
sleep(WAIT_TIME)
while True:
    try:
        ep = es_pandas(es_host, http_auth=(credentials.elastic_username, credentials.elastic_password))
        break
    except :
        print('error')
        logger.warning("Connection to elasticsearch impossible. Next try in {} seconds".format(WAIT_TIME))
        sleep(WAIT_TIME)