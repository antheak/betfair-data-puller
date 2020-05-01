#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mar 17 2020

@author: tristanfulchiron
"""
import betfairlightweight
import credentials
import pandas as pd
import datetime
import runnerbooks_processing
import os
from data_puller_logger import logger
from data_puller_mysql import con
from data_puller_es import ep


# ------ CONSTANT VARIABLES -------
MAX_NUMBER_MARKET_IDS = 10
MAX_REQUEST_EVENT_IDS = 30
MINIMUM_TRADED_VOLUME_TO_REQUEST_DATA = 100
MAX_RESULT_MARKET_CATALOGUES = 100


def login():
    """
    Connects the user to the betfair API using his credentials (username, password, appkey), and his certificate.
    Both the credentials and the certificate path are hardcoded in function variables.
    :return:
    """
    directory = os.path.dirname(__file__)
    certs_path = os.path.join(directory, '../certs')
    my_username = credentials.betfair_api_username
    my_password = credentials.betfair_api_password
    my_app_key = credentials.betfair_api_app_key
    trading = betfairlightweight.APIClient(username=my_username,
                                           password=my_password,
                                           app_key=my_app_key,
                                           certs=certs_path)
    trading.login()
    return trading


class BetfairDataPuller:

    def __init__(self, trading, market_ids=[], event_ids_blacklist=[], size_available_new_market_ids=None):
        self.trading = trading
        self.market_ids = market_ids
        self.event_ids_blacklist = event_ids_blacklist
        self.size_available_new_market_ids = size_available_new_market_ids
        if size_available_new_market_ids is None:
            self.size_available_new_market_ids = MAX_NUMBER_MARKET_IDS
        self.events = None
        self.market_catalogues = None
        self.price_data_projection = ['EX_BEST_OFFERS']
        self.runner_names_df = pd.DataFrame({
            'Selection ID': [],
            'Runner Names': [],
        })
        self.market_info_df = pd.DataFrame({
            'Market ID': [],
            'Country Code': [],
            'Competition Name': [],
            'Competition ID': []
        })
        self.results_df = pd.DataFrame({
            'Market ID': [],
            'Winner Selection ID': []
        })
        self.market_data_df = pd.DataFrame()

    def restart(self, trading):
        """
        Allows to restart the instance with a new trading input, only keeping the running market ids. This is necessary
        because the login expires on the betfair API after 24 hours, but we don't want to lose track of the on-going
        matches when restarting.
        :param trading: the trading object of the betfairlightweight APIClient.
        :return:
        """
        self.__init__(trading, self.market_ids, self.event_ids_blacklist, self.size_available_new_market_ids)

    @staticmethod
    def get_event_filter(event_type_id, minutes_before_starting_event):
        """
        Gets a filter for the list_events Betfair API.
        :param event_type_id: int
            Betfair id of the event type we want to pull (e.g. 1 for soccer).
        :param minutes_before_starting_event: int
            The time delta before the beginning of the event and the beginning of data crawling (e.g. for 120, we start
            pulling the market data 2 hours before the game starts).
        :return:
        A filter from the betfairlightweight class, to be used in the Betfair list_events API.
        """
        event_filter = betfairlightweight.filters.market_filter(
            event_type_ids=[event_type_id],
            market_start_time={
                'from': datetime.datetime.utcnow().strftime("%Y-%m-%dT%TZ"),
                'to': (datetime.datetime.utcnow() + datetime.timedelta(minutes=minutes_before_starting_event)).strftime(
                    "%Y-%m-%dT%TZ")
            },
            turn_in_play_enabled=True,
            in_play_only=False,
            market_betting_types=['ODDS']
        )
        return event_filter

    def get_events(self, event_type_id, minutes_before_starting_event):
        """
        Calls the Betfair list_events API to retrieve an event list based on the self.event_filter.
        :param event_type_id:
        :param minutes_before_starting_event:
        :return:
        """
        self.events = self.trading.betting.list_events(
            filter=self.get_event_filter(event_type_id=event_type_id,
                                         minutes_before_starting_event=minutes_before_starting_event)
        )

    @property
    def get_events_candidates(self):
        """
        Computes a list of event_ids to be used in the list_market_catalogue Betfair API, with respect to the maximum
        number of events requestable.
        :return:
        """
        if self.events is None:
            raise ValueError('The events should be retrieved first. Please run the getEvents function in that purpose')
        return [event_object.event.id for event_object in self.events][:MAX_REQUEST_EVENT_IDS - 1]

    @staticmethod
    def get_market_catalogue_filter(event_ids, market_type_codes):
        """
        Returns the corresponding betfairlightweight filter object
        :param event_ids: list of strings
        :param market_type_codes: list of strings
            Restricts to markets that match the type of the market (i.e. MATCH_ODDS, HALF_TIME_SCORE)
        :return:
        """
        return betfairlightweight.filters.market_filter(event_ids=event_ids, market_type_codes=market_type_codes)

    def get_market_catalogues(self, market_type_codes,
                              market_projection=None):
        """
        Calls the getMarketCatalogueFilter function on the getEventsCandidates list, and retrieves the available market
        catalogues from the Betfair list_market_catalogue API accordingly.
        :param market_type_codes: list of strings
            Restricts to markets that match the type of the market (i.e. MATCH_ODDS, HALF_TIME_SCORE)
        :param market_projection: list of strings
            The type and amount of data returned about the markets. Default ['EVENT', 'MARKET_START_TIME',
             'RUNNER_METADATA', 'COMPETITION']
        :return:
        """
        if market_projection is None:
            market_projection = ['EVENT', 'MARKET_START_TIME', 'RUNNER_METADATA', 'COMPETITION']

        if self.events == []:
            self.market_catalogues = []
        else:
            self.market_catalogues = self.trading.betting.list_market_catalogue(
                filter=self.get_market_catalogue_filter(self.get_events_candidates, market_type_codes),
                market_projection=market_projection,
                max_results=str(MAX_RESULT_MARKET_CATALOGUES),
                sort='MAXIMUM_TRADED'
            )

    def _save_runner_names(self, runners_catalogues):
        """
        Parse the runners_catalogues argument to Update of the "known names of the teams" DataFrame(runner_names_df)
        :param runners_catalogues: A list of Runners objects from Betfair API
        :return:
        """
        selection_ids, runner_names = [], []
        [selection_ids.extend([runner.selection_id for runner in runners_catalogue]) for runners_catalogue in
         runners_catalogues]
        [runner_names.extend([runner.runner_name for runner in runners_catalogue]) for runners_catalogue in
         runners_catalogues]
        current_runner_names_df = pd.DataFrame({
            'Selection ID': selection_ids,
            'Runner Names': runner_names,
        })
        current_runner_names_df = current_runner_names_df.loc[
            current_runner_names_df['Selection ID'] != 58805.0
            ]
        self.runner_names_df = self.runner_names_df.append(current_runner_names_df)

    @property
    def _parse_market_catalogues(self):
        """Creates a DataFrame from the market catalogues."""
        market_catalogues_df = pd.DataFrame({
            # some column require an if condition in case one market_cat_object's attribute is None
            'Country Code': [market_cat_object.event.country_code if market_cat_object.competition is not None
                             else None for market_cat_object in self.market_catalogues],
            'Competition Name': [market_cat_object.competition.name if market_cat_object.competition is not None
                                 else None for market_cat_object in self.market_catalogues],
            'Competition ID': [market_cat_object.competition.id if market_cat_object.competition is not None
                               else None for market_cat_object in self.market_catalogues],
            'Market ID': [market_cat_object.market_id for market_cat_object in self.market_catalogues],
            'Total Matched': [market_cat_object.total_matched for market_cat_object in self.market_catalogues],
            'Event ID': [market_cat_object.event.id if market_cat_object.event is not None
                         else None for market_cat_object in self.market_catalogues],
            'Runners': [market_cat_object.runners for market_cat_object in self.market_catalogues]
        })
        return market_catalogues_df

    @property
    def _filter_market_catalogues(self):
        """Filters the Market Catalogues DataFrame according to the Minimum Traded Volume and the blacklisted event ids
        variables."""
        market_catalogues_df = self._parse_market_catalogues
        # Filter the DataFrame based on ids already in the event_ids_blacklist
        market_catalogues_df = market_catalogues_df.loc[
            ~market_catalogues_df['Event ID'].isin(self.event_ids_blacklist)
        ]
        # Filter the DataFrame based on the Minimum Traded Volume
        market_catalogues_df = market_catalogues_df.loc[
            market_catalogues_df['Total Matched'] > MINIMUM_TRADED_VOLUME_TO_REQUEST_DATA]

        return market_catalogues_df

    def update_market_ids(self):
        """
        Updates the market_ids list used to retrieve market data from the Betfair API, with the new selected market ids.
        Updates other important variables used to get this new list (event_ids_blacklist,
        size_available_new_market_ids).
        Updates the DataFrame containing the name of the teams that have been selected.
        Updates the DataFrame with the static information about the markets we want to save
        :return:
        """
        market_catalogues_df = self._filter_market_catalogues
        new_market_ids = list(market_catalogues_df['Market ID'])[0:self.size_available_new_market_ids - 1]

        # Update the market_ids and event_ids_blacklist lists
        self.market_ids.extend(new_market_ids)
        self.event_ids_blacklist.extend(
            list(market_catalogues_df['Event ID'])[0:self.size_available_new_market_ids - 1])
        logger.info("The market_ids list has been updated with {} new market_ids".format(str(len(new_market_ids))))

        # Update the DataFrame with the name of the teams
        self._save_runner_names(list(market_catalogues_df['Runners'])[0:self.size_available_new_market_ids - 1])

        # Update the DataFrame with the static info on the markets
        new_market_info_df = market_catalogues_df[
                                 ['Market ID', 'Country Code', 'Competition Name', 'Competition ID']
                             ].iloc[
                             0:self.size_available_new_market_ids - 1
                             ]
        self.market_info_df = self.market_info_df.append(new_market_info_df)

        # Update the size available for potential new market ids
        self.size_available_new_market_ids = MAX_NUMBER_MARKET_IDS - len(self.market_ids)

    @property
    def _get_market_data_filter(self):
        return betfairlightweight.filters.price_projection(
            price_data=self.price_data_projection
        )

    @property
    def _get_market_books(self):
        """
        Calls the list_market_book Betfair API.
        :return: marketbooks and a datetime object containing the time when the API was requested.
        """
        return self.trading.betting.list_market_book(
            market_ids=self.market_ids,
            price_projection=self._get_market_data_filter
        ), datetime.datetime.utcnow().replace(microsecond=0)

    def update_market_data(self):
        """
        The main function of the class. Fetches the  market books for the current market ids. Leverages them by getting
        the result of the game if the market is closed, or by processing them to feed the market data if the game is
        still in play.
        :return:
        """
        market_books, date = self._get_market_books
        for market_book in market_books:
            if market_book.status == 'CLOSED':
                winner = self._get_result(market_book)
                if winner is not None:
                    self.market_ids.remove(market_book.market_id)

            else:
                temp_market_data = runnerbooks_processing.process_runner_books(market_book.runners, market_book.status,
                                                                               market_book.inplay,
                                                                               market_book.market_id, date)
                self.market_data_df = self.market_data_df.append(
                    temp_market_data
                ).reset_index(drop=True)

    def _get_result(self, market_book):
        """
        Retrieves the winner from a market book, when the corresponding market has been previously detected as 'CLOSED'.
        Updates the results DataFrame of the class accordingly.
        :param market_book: The market book to retrieve the results from
        :return:
        """
        winner = None
        for runner in market_book.runners:
            if runner.status == 'WINNER':
                winner = runner.selection_id
        if winner is not None:
            self.results_df = self.results_df.append(
                pd.DataFrame({
                    'Market ID': market_book.market_id,
                    'Winner Selection ID': [winner]
                })
            )
            logger.info("The result file has been updated with game {} and winner {}".format(market_book.market_id,
                                                                                             winner))
        else:
            logger.warning("Unable to get the result for game {}".format(market_book.market_id))
        return winner

    def write_data(self, runner_names_csv, market_info_csv, results_csv, market_data_csv, to_csv=True, to_mysql=True,
                   to_es=False):
        len_data_written = len(self.runner_names_df) + len(self.market_info_df) + len(self.results_df) +\
                           len(self.market_data_df)

        if to_csv:
            self.runner_names_df.to_csv(runner_names_csv, mode='a', header=False, index=False)
            self.market_info_df.to_csv(market_info_csv, mode='a', header=False, index=False)
            self.results_df.to_csv(results_csv, mode='a', header=False, index=False)
            self.market_data_df.to_csv(market_data_csv, mode='a', header=False, index=False)

        if to_es:
            ep.to_es(self.runner_names_df, index='data-runner-names')
            ep.to_es(self.market_info_df, index='data-market-info')
            ep.to_es(self.results_df, index='data-results')
            ep.to_es(self.market_data_df, index='data-market-data')

        if to_mysql:
            self.runner_names_df.columns = [x.replace(' ', '') for x in self.runner_names_df.columns]
            self.market_info_df.columns = [x.replace(' ', '') for x in self.market_info_df.columns]
            self.results_df.columns = [x.replace(' ', '') for x in self.results_df.columns]
            self.market_data_df.columns = [x.replace(' ', '') for x in self.market_data_df.columns]
            self.runner_names_df.to_sql('runnerNames', con, if_exists='append', index=False)
            self.market_info_df.to_sql('marketInfo', con, if_exists='append', index=False)
            self.results_df.to_sql('results', con, if_exists='append', index=False)
            self.market_data_df.to_sql('marketData', con, if_exists='append', index=False)

        self.runner_names_df = pd.DataFrame()
        self.market_info_df = pd.DataFrame()
        self.results_df = pd.DataFrame()
        self.market_data_df = pd.DataFrame()

        logger.info("{} lines of data have been written".format(len_data_written))


