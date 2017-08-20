# -*- coding: utf-8 -*-
'''
File:
    get_stocks_historical_data.py
    Created on 2017/08/20

Description:
    Download Chinese stocks historical data through tushare interface.

Auther:
    QuantFisher

Contact:
    QuantFisher@gmail.com
'''
import os
import time
import pandas as pd
import tushare as ts
import datetime as dt
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

pd.set_option('display.width', 1000)

today = dt.date.today().strftime("%Y%m%d")
startDate = '2016-01-01'
endDate = '2016-12-31'
infoFilePath = "..\\data\\cn\\"
if not os.path.exists(infoFilePath):
    try:
        os.makedirs(infoFilePath)
    except OSError:
        if not os.path.isdir(infoFilePath):
            raise Exception
hisDataPath = "..\\data\\history\\2016\\"
if not os.path.exists(hisDataPath):
    try:
        os.makedirs(hisDataPath)
    except OSError:
        if not os.path.isdir(hisDataPath):
            raise Exception


def get_stocks_basics_info():
    file = infoFilePath + "stk_basics_" + today + ".csv"
    if os.path.exists(file) is not True:
        try:
            raw_data = ts.get_stock_basics()
        except IOError:
            raise Exception('Can not read stocks basic info through tushare interface!')

        raw_data = raw_data.sort_index()

        try:
            raw_data.to_csv(file, index=True, encoding='gbk')
        except Exception:
            print('Write stocks basic info to csv file error!')

    try:
        raw_data = pd.read_csv(file, encoding='gbk')
    except IOError:
        print('Read stocks basic info from csv file error!')

    return raw_data


def get_stock_daily_trading_data(code, start_date=startDate, end_date=endDate):
    code = str(code).zfill(6)
    try:
        stock_data = ts.get_h_data(str(code), start=start_date, end=end_date, autype=None)
        filename = hisDataPath + code + ".csv"
        stock_data.to_csv(filename, index=True, encoding='gbk')
        print('Get stock:', code, 'historical data done!')
    except Exception:
        print('Failed to get stock:', code, 'historical data!')
        time.sleep(30)
        get_stock_daily_trading_data(code, startDate, endDate)


def get_all_stocks_historical_data():
    print("Start to get all stocks' historical data!")

    stockinfo = get_stocks_basics_info()

    for index in stockinfo.index:
        ipo_date = stockinfo.ix[index]['timeToMarket']  # ipo date: YYYYMMDD
        if ipo_date != 0 and ipo_date < 20170101:
            code = stockinfo.ix[index]['code']
            get_stock_daily_trading_data(code, startDate, endDate)

    print("Get all stocks' historical data done!")


def get_all_stocks_historical_data_multithread():
    print("Start to get all stocks' historical data!")

    try:
        stockinfo = get_stocks_basics_info()
        pool = ThreadPool(processes=5)
        pool.map(get_stock_daily_trading_data, stockinfo['code'])
        pool.close()
        pool.join()

    except Exception as e:
        print(str(e))

    print("Get all stocks' historical data done!")


def main():
    #stockinfo = get_stocks_basics_info()
    #get_stock_daily_trading_data('000839', startDate, '2017-1-1')
    #get_all_stocks_historical_data()
    get_all_stocks_historical_data_multithread()


if __name__ == "__main__":
    main()
