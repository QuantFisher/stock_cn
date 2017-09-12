'''
File:
    get_stocks_realtime_quotes.py
    Created on 2017/09/08

Description:
    Download Chinese stocks realtime quotes through tushare interface.
    3 seconds per tick data.

Auther:
    QuantFisher

Contact:
    QuantFisher@gmail.com
'''

import time
import datetime as dt
from dateutil.parser import parse
import tushare as ts
import pandas as pd
import os

pd.set_option('display.width', 1000)

stock_list = ('601992',
              '000413',
              '300009',
              )

data_list = [pd.DataFrame([]) for i in range(len(stock_list))]

seconds_per_tick_data = 3

data_rootdir = "..\\data\\quotes"
if not os.path.exists(data_rootdir):
    try:
        os.makedirs(data_rootdir)
    except OSError:
        if not os.path.isdir(data_rootdir):
            raise Exception

market_open_time1 = parse('09:15:00').time()
market_close_time1 = parse('09:25:00').time()
market_open_time2 = parse('09:30:00').time()
market_close_time2 = parse('11:30:00').time()
market_open_time3 = parse('13:00:00').time()
market_close_time3 = parse('15:00:00').time()
sched_time = market_open_time1


def is_market_open(now):
    global sched_time

    if (market_open_time1 <= now <= market_close_time1) or \
       (market_open_time2 <= now <= market_close_time2) or \
       (market_open_time3 <= now <= market_close_time3):
        print('Market Open! Current time:', now.strftime("%H:%M:%S"))
        return True
    elif now < market_open_time1:
        sched_time = market_open_time1
        print('Market close! Current time:', now.strftime("%H:%M:%S"))
        print('Wait until market open!')
        return False
    elif market_close_time1 < now < market_open_time2:
        sched_time = market_open_time2
        print('Market close! Current time:', now.strftime("%H:%M:%S"))
        print('Wait until market open!')
        return False
    elif market_close_time2 < now < market_open_time3:
        sched_time = market_open_time3
        print('Market close! Current time:', now.strftime("%H:%M:%S"))
        print('Wait until market open!')
        return False
    elif now > market_close_time3:
        print('Market close! Current time:', now.strftime("%H:%M:%S"))
        return False


def process_raw_data(raw_data):
    # process raw data
    raw_data['time'] = dt.datetime.now().time().strftime("%H:%M:%S")

    return raw_data


def save_to_csv(data_list):
    today = dt.date.today().strftime("%Y%m%d")
    directory = data_rootdir + "\\zb_level1_" + today + "\\"
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except OSError:
            if not os.path.isdir(directory):
                raise

    for i in range(len(stock_list)):
        filepath = directory + stock_list[i] + ".csv"
        #data_list[i].reset_index()
        try:
            data_list[i].to_csv(filepath, encoding='gb2312', date_format='str', index=False)
        except Exception:
            print('Error in writing data to CSV file! File name:', stock_list[i], '\n')
            filepath = ".\\" + stock_list[i] + ".csv"
            data_list[i].to_csv(filepath, encoding='gb2312', date_format='str', index=False)
            continue


def get_stock_quotes():
    # Get real time quotes of the stock list
    global sched_time

    while True:
        now = dt.datetime.now().time()
        #now = dt.time(9, 25, 1)
        if is_market_open(now):
            try:
                raw_data = ts.get_realtime_quotes(stock_list)
                raw_data = process_raw_data(raw_data)
                #print(raw_data)
                for index, row in raw_data.iterrows():
                    data_list[index] = pd.concat([data_list[index], raw_data[index:index + 1]])
                    #print(data_list[index])
                print('Get real time quotes of stock list done!')
                time.sleep(seconds_per_tick_data)
            except KeyboardInterrupt:
                print('Interrupt occur!')
                break
            except Exception:
                print('Error in reading!')
                continue
        else:
            if now > market_close_time3:
                break
            else:
                sleep_time = (sched_time.hour - now.hour) * 60 * 60 + (sched_time.minute - now.minute) * 60 + (sched_time.second - now.second)
                print(sleep_time)
                time.sleep(sleep_time)
    save_to_csv(data_list)


def main():
    get_stock_quotes()


if __name__ == "__main__":
    main()
