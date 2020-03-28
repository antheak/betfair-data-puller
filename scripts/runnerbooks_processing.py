#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mar 14 2020

@author: tristanfulchiron
"""
import pandas as pd
import numpy as np

DATA_DEPTH = 3


def process_runner_books(runner_books, market_status, in_play, market_id, date):
    '''
    This function processes the runner books and returns a DataFrame with the best back/lay prices + vol for each
    runner.
    :param date: The date to add to the returned DataFrame.
    :param market_id: The market_id to add to the returned DataFrame.
    :param in_play: The in_play status to add to the returned DataFrame.
    :param market_status: The markets_status to add to the returned DataFrame.
    :param runner_books: The runner books to be processed.
    :return:
    '''

    back_prices = [[runner_book.ex.available_to_back[i].price
                    if i < len(runner_book.ex.available_to_back)
                    else 1.0
                    for i in range(DATA_DEPTH)]
                   for runner_book
                   in runner_books]

    back_sizes = [[runner_book.ex.available_to_back[i].size
                   if i < len(runner_book.ex.available_to_back)
                   else 1.0
                   for i in range(DATA_DEPTH)]
                  for runner_book
                  in runner_books]

    lay_prices = [[runner_book.ex.available_to_lay[i].price
                   if i < len(runner_book.ex.available_to_lay)
                   else 1.0
                   for i in range(DATA_DEPTH)]
                  for runner_book
                  in runner_books]

    lay_sizes = [[runner_book.ex.available_to_lay[i].size
                  if i < len(runner_book.ex.available_to_lay)
                  else 1.0
                  for i in range(DATA_DEPTH)]
                 for runner_book
                 in runner_books]

    # If there are no prices, no need to go further
    if (back_prices == [[], [], []]) & (lay_prices == [[], [], []]):
        return pd.DataFrame()

    back_price_1 = np.array(back_prices)[:, 0]
    back_price_2 = np.array(back_prices)[:, 1]
    back_price_3 = np.array(back_prices)[:, 2]
    back_sizes_1 = np.array(back_sizes)[:, 0]
    back_sizes_2 = np.array(back_sizes)[:, 1]
    back_sizes_3 = np.array(back_sizes)[:, 2]
    lay_price_1 = np.array(lay_prices)[:, 0]
    lay_price_2 = np.array(lay_prices)[:, 1]
    lay_price_3 = np.array(lay_prices)[:, 2]
    lay_sizes_1 = np.array(lay_sizes)[:, 0]
    lay_sizes_2 = np.array(lay_sizes)[:, 1]
    lay_sizes_3 = np.array(lay_sizes)[:, 2]

    selection_ids = [runner_book.selection_id for runner_book in runner_books]
    last_prices_traded = [runner_book.last_price_traded for runner_book in runner_books]
    market_status = [market_status] * len(selection_ids)
    in_play = [in_play] * len(selection_ids)
    market_id = [market_id] * len(selection_ids)
    date = [date] * len(selection_ids)

    df = pd.DataFrame({
        'Selection ID': selection_ids,
        'Back Price 1': back_price_1,
        'Back Size 1': back_sizes_1,
        'Back Price 2': back_price_2,
        'Back Size 2': back_sizes_2,
        'Back Price 3': back_price_3,
        'Back Size 3': back_sizes_3,
        'Lay Price 1': lay_price_1,
        'Lay Size 1': lay_sizes_1,
        'Lay Price 2': lay_price_2,
        'Lay Size 2': lay_sizes_2,
        'Lay Price 3': lay_price_3,
        'Lay Size 3': lay_sizes_3,
        'Last Price Traded': last_prices_traded,
        'Market Status': market_status,
        'In Play': in_play,
        'Market ID': market_id,
        'Date': date
    })
    return df
