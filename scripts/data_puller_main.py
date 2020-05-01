#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mar 17 2020

@author: tristanfulchiron
"""

import betfair_data_puller
from time import sleep
import pandas as pd
from data_puller_logger import logger
import traceback
import os
import sys


# ------ CONSTANT VARIABLES -------
MINUTES_BEFORE_STARTING_EVENT = 120
LATENCY_PERIOD = 30
LATENCY_MULTIPLYING_FACTOR_EVENTS = 10
LATENCY_MULTIPLYING_FACTOR_ERROR = 20
NUMBER_OF_TRY_BEFORE_LATENCY_ERROR = 10
runner_names_csv = '../data/runner_names.csv'
market_info_csv = '../data/market_info.csv'
results_csv = '../data/results.csv'
market_data_csv = '../data/market_data.csv'

# ------ CREATE EMPTY CSVs -------
if not os.path.exists(runner_names_csv):
    pd.DataFrame({
                'Selection ID': [58805.0],
                'Runner Names': ['The Draw'],
            }).to_csv(runner_names_csv, sep=',', index=False)

if not os.path.exists(market_info_csv):
    pd.DataFrame({
                'Market ID': [],
                'Country Code': [],
                'Competition Name': [],
                'Competition ID': []
            }).to_csv(market_info_csv, sep=',', index=False)

if not os.path.exists(results_csv):
    pd.DataFrame({
                'Market ID': [],
                'Winner Selection ID': []
            }).to_csv(results_csv, sep=',', index=False)

if not os.path.exists(market_data_csv):
    pd.DataFrame({
                'Selection ID': [],
                'Back Price 1': [],
                'Back Size 1': [],
                'Back Price 2': [],
                'Back Size 2': [],
                'Back Price 3': [],
                'Back Size 3': [],
                'Lay Price 1': [],
                'Lay Size 1': [],
                'Lay Price 2': [],
                'Lay Size 2': [],
                'Lay Price 3': [],
                'Lay Size 3': [],
                'Last Price Traded': [],
                'Market Status': [],
                'In Play': [],
                'Market ID': [],
                'Date': []
            }).to_csv(market_data_csv, sep=',', index=False)


def main(bdp):
    """
    Implements the infinite while loop : The market data are requested every LATENCY_PERIOD seconds. The events are
    requested every LATENCY_PERIOD*LATENCY_MULTIPLYING_FACTOR to search for new market_ids to add in the loop.
    In case there is an error during the loop, the main enters in the except part, and re-tries to enter the loop after
    the LATENCY_PERIOD.
    :return:
    """
    count = 0
    count_error = 0
    while True:
        try:
            if count % LATENCY_MULTIPLYING_FACTOR_EVENTS == 0:
                # New markets_ids can be added to the loop only if there is some place left regarding the API
                # limitations.
                if bdp.size_available_new_market_ids == 0:
                    logger.warning("There is no place left to add new market_ids")
                    #If there is no place left, we want to make sure we come back to this point in the loop every
                    # LATENCY_PERIOD to check is there is still no place left.
                    count -= 1
                else:
                    bdp.get_events(event_type_id=1, minutes_before_starting_event=MINUTES_BEFORE_STARTING_EVENT)
                    bdp.get_market_catalogues(market_type_codes=['MATCH_ODDS'])
                    bdp.update_market_ids()
            bdp.update_market_data()
            bdp.write_data(runner_names_csv, market_info_csv, results_csv, market_data_csv, to_csv=True, to_es=True)
            count += 1
            count_error = 0
            logger.info("Loop successfully ended with {} running market ids".format(len(bdp.market_ids)))
            sleep(LATENCY_PERIOD)
        except Exception as inst:

            if 'INVALID_SESSION_INFORMATION' in inst.args[0]:
                # This is a known error. After 24 hours, the login to the betfair API expires, so there is a need to
                # restart the bdp object.
                logger.warning("Session expired. Reconnection to the API occurring...")
                trading_local = betfair_data_puller.login()
                bdp.restart(trading_local)

            else:
                count_error += 1
                if count_error < NUMBER_OF_TRY_BEFORE_LATENCY_ERROR:
                    logger.error("Something went wrong during the loop. Next try in {} seconds".format(LATENCY_PERIOD))
                    sleep(LATENCY_PERIOD)
                else:
                    sleep(LATENCY_PERIOD*LATENCY_MULTIPLYING_FACTOR_ERROR)
                    logger.error("Something went wrong during the loop. "
                                 "Next try in {} seconds".format(LATENCY_PERIOD*LATENCY_MULTIPLYING_FACTOR_ERROR))
                logger.error(type(inst))
                logger.error(inst.args)
                logger.error(traceback.format_exc())


# ------ INITIALIZATION -------
try:
    trading = betfair_data_puller.login()
    BDP = betfair_data_puller.BetfairDataPuller(trading)
except Exception as inst:
    logger.error(type(inst))
    logger.error(inst.args)
    logger.error(traceback.format_exc())
    sys.exit()

if __name__ == "__main__":
    main(BDP)
