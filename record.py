#!/usr/bin/env python3

# This code is using Kraken API to retrieve and save trade info for a given pair every single minute:
# - Open,High,Low,Close
# - Trades
# - Orders' depths

import krakenex
import time
from datetime import datetime
from pathlib import Path
import os


def get_OHLC(pair, last2min, slept_time, err_sleep_time):
    # - pair: 'XXBTZEUR', 'XXBTZUSD', 'XXBTZCAD', 'XXBTZGBP', 'XXBTZJPY'
    # - las2min: is the time integer since which to get data
    # - err_sleep_time: time to sleep when error occurs

    n_tries = 1
    no_success = True
    while no_success:
        try:
            OHLC_res = k.query_public('OHLC', data = {'pair': pair, 'since': last2min})
            last = OHLC_res['result']['last'] # last corresponds to [-2] compnent (see below for explanation)
            bars = OHLC_res['result'][pair]
            OHLC_str = ''
            # - append OHLC into one string. [-2] component is the one already completed, [-1] is the one that is still appending - NOTE: can use [-1] for tracking on-the-fly, also OHLC data for the returned time integer corresponds to what was accumulated starting from the this time + 60sec
            for val in bars[-2]: OHLC_str = OHLC_str + str(val) + ','
            no_success = False
        except:
            print(f'error in {pair}-OHLC at time:', last2min, 'trial', n_tries)
            n_tries = n_tries + 1
            time.sleep(err_sleep_time)
            slept_time = slept_time + err_sleep_time
            pass

    return [OHLC_str, last, slept_time]


def get_Trades(pair, last2min, OHLC_last, slept_time, err_sleep_time):
    # - pair: 'XXBTZEUR', 'XXBTZUSD', 'XXBTZGBP', 'XXBTZCAD', 'XXBTZJPY'
    # - las2min: is the time integer since which to get data
    # - err_sleep_time: time to sleep when error occurs

    n_tries = 1
    no_success = True
    while no_success:
        try:
            Trades_res = k.query_public('Trades', {'pair': pair, 'since': last2min})
            count = 0
            # - append to a string the time integer the trades correspond to
            Trades_str = str(OHLC_last) + ','
            # - append to a string the trades completed between the completed time integer and the ongoing one (i.e., [-2] and [-1], see OHLC comments for the time integers)
            for trade in Trades_res['result'][pair]:
                if (trade[2] >= float(OHLC_last)) and (trade[2] < float(OHLC_last+60)):
                    for var in trade: Trades_str = Trades_str + str(var) + ','
                    count = count + 1
            no_success = False
        except:
            print(f'error in {pair}-Trades at time:', last2min, 'trial', n_tries)
            n_tries = n_tries + 1
            time.sleep(err_sleep_time)
            slept_time = slept_time + err_sleep_time
            pass

    return [Trades_str, slept_time]


def get_Depth(pair, last2min, OHLC_last, slept_time, err_sleep_time, depth_count):
    # - pair: 'XXBTZEUR', 'XXBTZUSD', 'XXBTZGBP', 'XXBTZCAD', 'XXBTZJPY'
    # - las2min: is the time integer since which to get data
    # - err_sleep_time: time to sleep when error occurs

    n_tries = 1
    no_success = True
    while no_success:
        try:
            Depth_res = k.query_public('Depth', {'pair': pair, 'count': str()})
            Depth_str = str(OHLC_last) + ','
            for ask in Depth_res['result'][pair]['asks']:
                for var in ask: Depth_str = Depth_str + str(var) + ','
            for bid in Depth_res['result'][pair]['bids']:
                for var in bid: Depth_str = Depth_str + str(var) + ','
            no_success = False
        except:
            print(f'error in {pair}-Depths at time:', last2min, 'trial', n_tries)
            n_tries = n_tries + 1
            time.sleep(err_sleep_time)
            slept_time = slept_time + err_sleep_time
            pass

    return [Depth_str, slept_time]



if __name__ == '__main__':

    k = krakenex.API()
    init_time = time.time()
    new_start = True

    PAIRS = ['XXBTZEUR', 'XXBTZUSD', 'XXBTZGBP', 'XXBTZCAD', 'XXBTZJPY']
    sleep_time = 1.0
    depth_count = 10

    OHLC_next = time.time()

    # second last minute data
    while True:

        # CREATE DIRECTORY, IF DOES NOT EXIST
        year  = str(datetime.fromtimestamp(time.time()).year)
        month = str(datetime.fromtimestamp(time.time()).month)
        day   = str(datetime.fromtimestamp(time.time()).day)
        hour  = str(datetime.fromtimestamp(time.time()).hour)
        file_dir = "data/" + year + '/' + month + '/' + day + '/' + hour + '/'
        Path(file_dir).mkdir(parents=True, exist_ok=True)
        # START APPENDING INTO HEADER FILE, IF NOT IN THE FOLDER
        if (os.path.isfile(file_dir + 'header.csv') == False) or new_start:
            header_str = 'files starting with initial time:' + str(int(init_time)) + ','
            with open(file_dir + 'header.csv',"a") as f:
                f.write(header_str+'\n')
                f.flush()
                f.close()
            new_start = False

        slept_time = 0
        last2min = OHLC_next-120

        # FOR ALL PAIRS
        for pair in PAIRS:

            # - get OHLC
            [OHLC_str, OHLC_last, slept_time] = get_OHLC(pair=pair, last2min=last2min, slept_time=slept_time, err_sleep_time=sleep_time)
            # - append OHLC into file
            with open(file_dir + str(int(init_time))+'_OHLC_'+str(pair.split('Z')[1])+'.csv',"a") as f:
                f.write(OHLC_str+'\n')
                f.flush()
                f.close()

            # - get Trades
            [Trades_str, slept_time] = get_Trades(pair=pair, last2min=last2min, OHLC_last=OHLC_last, slept_time=slept_time, err_sleep_time=sleep_time)
            # - append Trades into file
            with open(file_dir + str(int(init_time))+'_Trades_'+str(pair.split('Z')[1])+'.csv',"a") as f:
                f.write(Trades_str+'\n')
                f.flush()
                f.close()

        # FOR EURO only
        pair = PAIRS[0]
        # - get Depth
        [Depth_str, slept_time] = get_Depth(pair=pair, last2min=last2min, OHLC_last=OHLC_last, slept_time=slept_time, err_sleep_time=sleep_time, depth_count=depth_count)
        # - append Depth into file
        with open(file_dir + str(int(init_time))+'_Depth_'+str(pair.split('Z')[1])+'.csv',"a") as f:
            f.write(Depth_str+'\n')
            f.flush()
            f.close()
        
        # HEADER FILE
        with open(file_dir + 'header.csv',"a") as f:
            header_str = str(OHLC_last) + ','
            f.write(header_str+'\n')
            f.flush()
            f.close()

        if slept_time > 55.0:
            print("slept for", slept_time, "secs, with last record at", OHLC_last)
        OHLC_next = OHLC_last + 60
        sleep_time = 61- int(int(time.time()) - OHLC_next)
        time.sleep(max(0.0,sleep_time))