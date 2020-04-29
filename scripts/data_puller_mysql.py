#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Apr 28 2020

@author: tristanfulchiron
"""

from data_puller_logger import logger
import credentials
from time import sleep
from sqlalchemy import create_engine
import traceback


mysql_host = 'mysql'
mysql_db = 'Betfair'
WAIT_TIME = 30

connection_string = 'mysql+pymysql://{}:{}@{}/{}'.format(credentials.mysql_user, credentials.mysql_password,
                                                         mysql_host, mysql_db)


# Not very elegant, but this is v0 is resilient, and allows the betfair container to wait for the mysql one to be
# set up before launching
sleep(WAIT_TIME)
while True:
    try:
        con = create_engine(connection_string).connect()
        print("connection succeed")
        break
    except Exception as inst:
        logger.warning("Connection to mysql impossible. Next try in {} seconds".format(WAIT_TIME))
        logger.error(type(inst))
        logger.error(inst.args)
        logger.error(traceback.format_exc())
        sleep(WAIT_TIME)

