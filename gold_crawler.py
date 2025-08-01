import json
import re
import sqlite3
import pandas as pd
import requests
import os
import subprocess
from bs4 import BeautifulSoup
import numpy as np
import websockets
import traceback
import asyncio
from datetime import datetime
from datetime import date
import time
import psycopg2
import jalali_pandas
import emoji
from telegram.error import TimedOut
import warnings
warnings.filterwarnings('ignore')
# from telegram.ext import Application
from concurrent.futures import ThreadPoolExecutor, TimeoutError
day_change=False
hour_change=False
alarm = 1
alarm_treshold=10
primary_alarm_treshold=50
secondary_alarm_treshold=20

# if need to send message to test chanel
test = False
n = 0
Alarm_send_ounce_num = 0
Alarm_send_num = 0
database_gold = 'gold_price_data.db'
repo_path = os.getcwd()
tokens = repo_path + '\\' + 'tokens.txt'

def read_config(tokens):
    config = {}
    with open(tokens, 'r') as file:
        for line in file:
            name, value = line.strip().split('=')
            name = name.strip()
            value = value.strip()
            config[name] = value
    return config

def send_file(token, chat_id, message, proxy=None):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": message
    }
    proxies = {
        "http": proxy,
        "https": proxy,
    } if proxy else None

    try:
        r = requests.post(url, data=data, proxies=proxies, timeout=10)
        if r.status_code == 200:
            print("✅ Message sent!")
        else:
            print(f"❌ Telegram API error: {r.status_code} {r.text}")
    except Exception as e:
        print(f"❌ Request error: {e}")
        try:
            requests.post(url, data=data, timeout=10)
        except:
            print("")

def send_telegram(message, test):

    try:
        proxy_url = "socks5h://127.0.0.1:1080"  # your SOCKS5 proxy
        config = read_config('tokens.txt')

        if test:
            TOKEN = config['TOKEN_TEST']
            CHAT_ID = config['CHAT_ID_TEST']
        else:
            TOKEN = config['TOKEN_GOLD']
            CHAT_ID = config['CHAT_ID_GOLD']
        send_file(TOKEN, CHAT_ID, message,proxy=proxy_url)
    except Exception as e:
        print("")
        # print(f"❌ Could not send message: {e}")


def send_telegram2(Times_min, df_jalalidate,positive24,positive1,now_mean,highest_price,lowest_price,highest_time,lowest_time,growth_24,growth_1, test):
    prohibited = emoji.emojize(":warning:")
    pos_neg_sign24 = ""
    pos_neg_sign1 = ""
    if positive24 == True:
        pos_neg_sign24 = '+'
    if positive24 == False:
        pos_neg_sign24 = ''
    if positive1 == True:
        pos_neg_sign1 = '+'
    if positive1 == False:
        pos_neg_sign1 = ''
    clock = emoji.emojize(':alarm_clock:')
    calendar = emoji.emojize(":spiral_calendar:")
    coin = emoji.emojize("")
    down = emoji.emojize(":down_arrow:")
    up = emoji.emojize(":up_arrow:")
    right = emoji.emojize(":right_arrow:")
    left = emoji.emojize(":left_arrow:")
    reminder = emoji.emojize(":pushpin:")
    white = emoji.emojize(":white_small_square:")
    black = emoji.emojize(":black_small_square:")
    print('pos_neg_sign24',pos_neg_sign24)
    print('pos_neg_sign1',pos_neg_sign1)
    pos_neg_sign24_situ= emoji.emojize(':green_circle:') + " "
    if pos_neg_sign24=="":
        pos_neg_sign24_situ= emoji.emojize(':red_circle:') + " "
    pos_neg_sign1_situ=  emoji.emojize(':green_circle:') + " "
    if pos_neg_sign1=="":
        pos_neg_sign1_situ=emoji.emojize(':red_circle:') + " "
    message =   (
                 # f'{prohibited} میانگین نرخ طلا:  {now_mean} تومان {growth_24_situ}\n\n'
                f'{prohibited} میانگین نرخ طلا:  {now_mean} تومان \n\n'
                f'درصد تغییرات 24 ساعته طلا\n'
                f'{pos_neg_sign24_situ} {pos_neg_sign24}{str(growth_24)} % \n\n'
                f'درصد تغییرات 1 ساعته طلا\n'
                f'{pos_neg_sign1_situ} {pos_neg_sign1}{str(growth_1)} % \n\n'
                 f'_______________________  \n\n'
                 f'{black} حداکثر و حداقل قیمت امروز:\n\n'
               f'          {up} {highest_price}           AT: {highest_time}\n\n'
               f'          {down} {lowest_price}           AT: {lowest_time}\n\n'
                 f'_______________________  \n'
                 f'                     {calendar}  {(jalali_date)}\n\n'
                 f'                           {clock}  {Times_min}\n'
                 f'\n'
                 f'@alarm_change_gold'
                 )

    # message = message.encode('utf-8')
    message = message.replace("]", "")
    message = message.replace("[", "")
    message = message.replace("'", "")

    proxy_url = "socks5h://127.0.0.1:1080"  # your SOCKS5 proxy
    config = read_config('tokens.txt')

    TOKEN = config['TOKEN_GOLD']
    if test:
        CHAT_ID = config['CHAT_ID_TEST']
    else:
        CHAT_ID = config['CHAT_ID_ALARM_gold']
    # CHAT_ID = config['CHAT_ID_ALARM_gold']
    send_file(TOKEN, CHAT_ID, message,proxy=proxy_url)


def send_telegram3(positive24_ounce_price, ounce_price,ounce_dif, test):
    prohibited = emoji.emojize(":warning:")
    pos_neg_sign24_ounce_price = ""
    if positive24_ounce_price == True:
        pos_neg_sign24_ounce_price = '+'
    if positive24_ounce_price == False:
        pos_neg_sign24_ounce_price = ''
    clock = emoji.emojize(':alarm_clock:')
    calendar = emoji.emojize(":spiral_calendar:")
    coin = emoji.emojize("")
    down = emoji.emojize(":down_arrow:")
    up = emoji.emojize(":up_arrow:")
    right = emoji.emojize(":right_arrow:")
    left = emoji.emojize(":left_arrow:")
    reminder = emoji.emojize(":pushpin:")
    white = emoji.emojize(":white_small_square:")
    black = emoji.emojize(":black_small_square:")
    print('pos_neg_sign24_ounce_price',pos_neg_sign24_ounce_price)
    pos_neg_sign24_ounce_price_situ= emoji.emojize(':green_circle:') + " "
    if pos_neg_sign24_ounce_price=="":
        pos_neg_sign24_ounce_price_situ= emoji.emojize(':red_circle:') + " "
    message =   (
                 # f'{prohibited} میانگین نرخ طلا:  {now_mean} تومان {growth_24_situ}\n\n'
                f'{prohibited} انس جهانی:  {ounce_price} دلار \n\n'
                f'درصد تغییرات 24 ساعته انس جهانی\n\n'
                f'{pos_neg_sign24_ounce_price_situ} {pos_neg_sign24_ounce_price}{str(ounce_dif)} % \n\n'
                 f'_______________________  \n'
                 f'                     {calendar}  {(jalali_date)}\n\n'
                 f'                           {clock}  {Times_min}\n'
                 f'\n'
                 f'@alarm_change_gold'
                 )

    # message = message.encode('utf-8')
    message = message.replace("]", "")
    message = message.replace("[", "")
    message = message.replace("'", "")
    proxy_url = "socks5h://127.0.0.1:1080"  # your SOCKS5 proxy
    config = read_config('tokens.txt')

    TOKEN = config['TOKEN_GOLD']
    if test:
        CHAT_ID = config['CHAT_ID_TEST']
    else:
        CHAT_ID = config['CHAT_ID_ALARM_gold']
    # CHAT_ID = config['CHAT_ID_ALARM_gold']
    send_file(TOKEN, CHAT_ID, message,proxy=proxy_url)



def time_function(n, time_start, sleep, ):
    df_today, df_jalalidate, = [[] for i in range(2)]
    timeStop_0 = datetime.now()
    time_end = timeStop_0.timestamp()
    timeStop = str(datetime.now())
    today = date.today()
    df_today.append(today)
    df_today = pd.to_datetime(pd.Series(df_today))
    jalali = df_today.jalali.to_jalali()
    year = jalali.jalali.year
    month = jalali.jalali.month
    day = jalali.jalali.day
    if len(str(month.values)) < 4:
        month='0'+month.astype(str)
    if len(str(day.values)) < 4:
        day = '0' + day.astype(str)
    jalali_date = year.astype(str) + "/" + month.astype(str) + "/" + day.astype(str)
    day=day.iloc[0]
    df_jalalidate.append(jalali_date.iloc[-1])
    Times_min = timeStop_0.strftime("%H:%M")
    Times_hour = timeStop_0.strftime("%H")

    dif_time = np.float64(time_end - time_start)
    time_next = sleep - dif_time
    print(f"{n}th, done")
    print(f"now, it's {timeStop}")
    return time_next, jalali_date, Times_hour, Times_min, df_jalalidate,day


def message_tel(Times_min, jalali_date, now_mean, rank_name_list, situ_list, rank_dif_list, rank_list, growth_month,
                growth_month_situ, growth_week, growth_week_situ, growth_24, growth_24_situ, growth_1, growth_1_situ,
                estjt_price, estjt_situ, estjt_dif, coin_price, coin_situ, coin_dif, ounce_price, ounce_situ, ounce_dif,
                dollar_price, dollar_situ, dollar_dif, dollar_based_price, dollar_based_situ, dollar_based_dif,
                bubble_situ, bubble,bubble_coin_situ, bubble_coin,bubble_coin_dif,dublicate_estjt,dublicate_ounce,dublicate_coin,
                highest_price,lowest_price,highest_time,lowest_time):
    clock = emoji.emojize(':alarm_clock:')
    calendar = emoji.emojize(":tear-off_calendar:")
    emoj = emoji.emojize("")
    yellow = emoji.emojize(":yellow_circle:")
    dollar = emoji.emojize(":heavy_dollar_sign:")
    down = emoji.emojize(":down_arrow:")
    up   =   emoji.emojize(":up_arrow:")
    right = emoji.emojize(":right_arrow:")
    left = emoji.emojize(":left_arrow:")
    reminder = emoji.emojize(":pushpin:")
    white = emoji.emojize(":white_small_square:")
    black = emoji.emojize(":black_small_square:")
    dublicate = ""
    if dublicate_estjt or dublicate_ounce or dublicate_coin:
        dublicate = "*"
    shift=""
    if len(bubble_coin) <10:
        shift="  "
    shift2=""
    if len(bubble) <9:
        shift2="  "


    if len(rank_list)>0:
        num0=f'{emoj} {rank_name_list[0]} :\n{rank_list[0]}          {situ_list[0]}  {rank_dif_list[0]} %\n\n'
    else:
        num0=""
    if len(rank_list)>1:
        num1=f'{emoj} {rank_name_list[1]} :\n{rank_list[1]}          {situ_list[1]}  {rank_dif_list[1]} %\n\n'
    else:
        num1=""
    if len(rank_list)>2:
        num2=f'{emoj} {rank_name_list[2]} :\n{rank_list[2]}          {situ_list[2]}  {rank_dif_list[2]} %\n\n'
    else:
        num2=""
    if len(rank_list)>3:
        num3=f'{emoj} {rank_name_list[3]} :\n{rank_list[3]}          {situ_list[3]}  {rank_dif_list[3]} %\n\n'
    else:
        num3=""
    if len(rank_list)>4:
        num4=f'{emoj} {rank_name_list[4]} :\n{rank_list[4]}          {situ_list[4]}  {rank_dif_list[4]} %\n\n'
    else:
        num4=""
    if len(rank_list)>5:
        num5=f'{emoj} {rank_name_list[5]} :\n{rank_list[5]}          {situ_list[5]}  {rank_dif_list[5]} %\n\n'
    else:
        num5=""
    if len(rank_list)>6:
        num6=f'{emoj} {rank_name_list[6]} :\n{rank_list[6]}          {situ_list[6]}  {rank_dif_list[6]} %\n\n'
    else:
        num6=""
    if len(rank_list)>7:
        num7=f'{emoj} {rank_name_list[7]} :\n{rank_list[7]}          {situ_list[7]}  {rank_dif_list[7]} %\n\n'
    else:
        num7=""
    if len(rank_list)>8:
        num8=f'{emoj} {rank_name_list[8]} :\n{rank_list[8]}          {situ_list[8]}  {rank_dif_list[8]} %\n\n'
    else:
        num8=""
    if len(rank_list)>9:
        num9=f'{emoj} {rank_name_list[9]} :\n{rank_list[9]}          {situ_list[9]}  {rank_dif_list[9]} %\n\n'
    else:
        num9=""
    if len(rank_list)>10:
        num10=f'{emoj} {rank_name_list[10]} :\n{rank_list[10]}          {situ_list[10]}  {rank_dif_list[10]} %\n\n'
    else:
        num10=""
    if len(rank_list)>11:
        num11=f'{emoj} {rank_name_list[11]} :\n{rank_list[11]}          {situ_list[11]}  {rank_dif_list[11]} %\n\n'
    else:
        num11=""
    if len(rank_list)>12:
        num12=f'{emoj} {rank_name_list[12]} :\n{rank_list[12]}          {situ_list[12]}  {rank_dif_list[12]} %\n'
    else:
        num12=""
    # if len(rank_list)>13:
    #     num13=f'{emoj} {rank_name_list[13]} :\n{rank_list[13]}          {situ_list[13]}  {rank_dif_list[13]} %\n'
    # else:
    #     num13=""



    # making message based on data
    message = ( '-------------------     {yellow} {yellow} {yellow}    -------------------\n'
                f' در کنار کانال فعلی، در دیگر کانال @alarm_change_gold صرفا تغییرات بیش از ۱ درصدی نرخ طلا و انس جهانی فرستاده می شود.\n\n'
                f' میانگین نرخ طلا:  {now_mean} تومان {growth_24_situ}\n\n'
               f'{black} حداکثر و حداقل قیمت امروز:\n\n'
               f'          {up} {highest_price}           AT: {highest_time}\n\n'
               f'          {down} {lowest_price}           AT: {lowest_time}\n'
               f'---------------  \n'
               f'{white} تغییر نسبت به ماه گذشته:\n'
               f'  {growth_month_situ} {growth_month} % \n\n'
               f'{white} تغییر نسبت به هفته گذشته:\n'
               f'  {growth_week_situ} {growth_week} % \n\n'
               f'{white} تغییر نسبت به روز گذشته:\n'
               f'  {growth_24_situ} {growth_24} % \n\n'
               f'{white} تغییر در یک ساعت گذشته:\n'
               f'  {growth_1_situ} {growth_1} % \n'
               f'---------------  \n'
               f'{reminder}قیمت های آنی و تغییرات 24 ساعته {down}\n'
               f'---------------  \n'
               f'       \n'
               f'{num0}'
               f'{num1}'
               f'{num2}'
               f'{num3}'
               f'{num4}'
               f'{num5}'
               f'{num6}'
               f'{num7}'
               f'{num8}'
               f'{num9}'
               f'{num10}'
               f'{num11}'
               # f'{num12}'
               f'_______________________  \n'
               f'{emoj} نرخ اتحادیه :\n{estjt_price}          {estjt_situ}  {estjt_dif} %\n\n'
               f'{emoj} نرخ سکه :\n{coin_price}       {coin_situ}  {coin_dif} %\n\n'
               f'{emoj} حباب سکه :\n{bubble_coin}   {shift}    {bubble_coin_situ}  {bubble_coin_dif} %\n\n'
               f'{emoj} انس جهانی :\n{ounce_price} {dollar}     {ounce_situ}  {ounce_dif} %\n\n'
               f'{emoj} دلار (تتر نوبیتکس) :\n\n({dollar_price})           {dollar_situ}  {dollar_dif} %\n\n'
               # f'{emoj} نرخ بر اساس انس جهانی و تتر :\n\n{dollar_based_price}         {dollar_based_situ}  {dollar_based_dif} %\n\n'
               # f'{emoj}                                                              :tgju\n{tgju_price}          {tgju_situ}  {tgju_dif} %\n\n'
               f'{emoj} حباب نرخ طلا (تومان) :\n\n             {shift2}            {bubble_situ} {bubble} \n'
               f'______________________  \n'
               f'                      {calendar}  {(jalali_date)}\n\n'
               f'                             {clock} {Times_min}\n'
               f'{dublicate}\n'
               f'@Gold_Iran_Online\n'
               )
    message = message.replace("]", "")
    message = message.replace("[", "")
    message = message.replace("'", "")
    return message


def sqlData(new_line):
    try:
        # Connect to myuserQL
        conn = psycopg2.connect(
            dbname='mydb',
            user='myuser',
            password='377843',
            host='localhost',
            port='5432'
        )
        cursor = conn.cursor()

        def sanitize(row):
            return [None if val == '' else val for val in row]

        new_line = sanitize(new_line)
        cursor.execute('''CREATE TABLE IF NOT EXISTS gold_data
                             (id TEXT,
                              hour text,
                              Time Text,
                              talasea INTEGER,
                              miligold INTEGER,
                              tlyn INTEGER,
                              daric INTEGER,
                              talapp INTEGER,
                              estjt INTEGER,
                              ap INTEGER,
                              tgju INTEGER,
                              melli INTEGER,
                              wallgold INTEGER,
                              technogold INTEGER,
                              digikala INTEGER,
                              zarpad INTEGER,
                              goldis INTEGER,
                              goldika INTEGER,                          
                              bazaretala INTEGER,
                              ounce INTEGER,
                              dollar INTEGER,
                              dollar_based INTEGER,
                              coin INTEGER,
                              bubble_coin INTEGER)''')
        insert_sql = '''
            INSERT INTO gold_data VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        cursor.execute(insert_sql, new_line)
        conn.commit()
        cursor.close()
        conn.close()
    except:
        pass
    try:
        con = sqlite3.connect(database_gold)
        cursor = con.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS gold_data
                                 (id TEXT,
                                  hour text
                                  Time Text,
                                  talasea INTEGER,
                                  miligold INTEGER,
                                  tlyn INTEGER,
                                  daric INTEGER,
                                  talapp INTEGER,
                                  estjt INTEGER,
                                  ap INTEGER,
                                  tgju INTEGER,
                                  melli INTEGER,
                                  wallgold INTEGER,
                                  technogold INTEGER,
                                  digikala INTEGER,
                                  zarpad INTEGER,
                                  goldis INTEGER,
                                  goldika INTEGER,                          
                                  bazaretala INTEGER,
                                  ounce INTEGER,
                                  dollar INTEGER,
                                  dollar_based INTEGER,
                                  coin INTEGER,
                                  bubble_coin INTEGER)''')
        cursor.execute("INSERT INTO gold_data VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", new_line)
        con.commit()
        con.close()
    except:
        pass

def history(n):
    # this function should used when whole app started in the middle of a day or when it's working and yet day has not changed

    try:
        try:
            conn = psycopg2.connect(
                dbname='mydb',
                user='myuser',
                password='377843',
                host='localhost',
                port='5432'
            )
            cur = conn.cursor()
        except Exception as e:
            print('postgre', e)
            conn = sqlite3.connect(database_gold)
            # Create a cursor object
            cur = conn.cursor()
            # Commit the changes
            conn.commit()
            # Execute a query to retrieve the data

        cur.execute("SELECT * FROM gold_data ORDER BY id DESC LIMIT 80000")  # Assuming 'id' is still your first column
        rows = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]

        history = pd.DataFrame(rows, columns=column_names)
        history = history.sort_values(by=history.columns[0], ascending=True)
        history_col=len(history.columns)
        days_history = history.groupby('id')
        """to calculate average of last weak and last month values"""
        if n == 1:
            try:
                formonthhistory = history.copy()
                formonthhistory.replace('', np.nan, inplace=True)
                formonthhistory = formonthhistory.bfill()
                # formonthhistory = formonthhistory.dropna()
                formonthhistory.index = formonthhistory['id']
                mon30 = formonthhistory.id.unique()[-31]
                mon0 = formonthhistory.id.unique()[-31]
                month = formonthhistory.loc[mon30:mon0]
                month_mean = month.iloc[:, 3:].astype(float).mean(axis=0)
                week7 = formonthhistory.id.unique()[-8]
                week0 = formonthhistory.id.unique()[-8]
                week = formonthhistory.loc[week7:week0]
                week_mean = week.iloc[:, 3:].astype(float).mean(axis=0)
            except Exception as e:
                print('sql month and weak, erorr, ', e)

        # getting yesterday to gathering a day before now's data
        group_day1 = history.id.unique()[-2]
        days_history1 = days_history.get_group(group_day1)
        days_history1 = days_history1.replace('', np.nan)
        days_history1.interpolate(method='linear', inplace=True)
        days_history1.interpolate(method='ffill', inplace=True)
        days_history1.interpolate(method='bfill', inplace=True)
        temp_mean = days_history1.dropna()
        temp_mean = temp_mean.iloc[:, 3:-5].astype(float).mean(axis=0).mean()
        days_history1.iloc[:, 3:-5] = days_history1.iloc[:, 3:-5].replace('', temp_mean)

        # separate today's data to get last hour data from it (without last 4 columns)
        group_day2 = history.id.unique()[-1]
        days_history2 = days_history.get_group(group_day2)
        days_history2 = days_history2.replace('', np.nan)
        days_history2.interpolate(method='linear', inplace=True)
        days_history2.interpolate(method='ffill', inplace=True)
        days_history2.interpolate(method='bfill', inplace=True)
        temp_mean = days_history2.dropna()
        temp_mean = temp_mean.iloc[:, 3:-5].astype(float).mean(axis=0).mean()
        days_history2.iloc[:, 3:-5] = days_history2.iloc[:, 3:-5].replace('', temp_mean)
        try:
            today_max,today_min=days_history2.iloc[:, 3:-5].mean(axis=1).max(),days_history2.iloc[:, 3:-5].mean(axis=1).min()
            highest_time = days_history2.loc[days_history2.iloc[:, 3:-5].mean(axis=1).idxmax(), 'time']
            lowest_time = days_history2.loc[days_history2.iloc[:, 3:-5].mean(axis=1).idxmin(), 'time']
        except Exception as e:
            print(e)
            today_max, today_min, highest_time, lowest_time = "", "", "", ""
        hours_history = days_history2.groupby('hour')
        last_two_hours = list(hours_history.groups.keys())
        last_two_hours = sorted([int(x) for x in last_two_hours])
        last_two_hours = last_two_hours[-2:]
        if n == 1:
            try:
                hours_history1 = hours_history.get_group(f"{int(last_two_hours[0]):02d}")
            except:
                hours_history1 = hours_history.get_group(int(last_two_hours[0]))
        else:
            try:
                hours_history1 = hours_history.get_group(f"{int(last_two_hours[-1]):02d}")
            except:
                hours_history1 = hours_history.get_group(int(last_two_hours[-1]))
        hours_history = hours_history1
        # calculate the mean of day date that is prepared before
        try:
            days_history = days_history1.iloc[:, 3:].astype(float).mean(axis=0)
        except:
            # here, if there is any problem with encoding or there is empty cells, try to handle it
            i_c = 3
            for i in days_history1.columns:
                days_history1.iloc[:, i_c] = pd.to_numeric(days_history1.iloc[:, i_c], errors="coerce")
                i_c += 1
                if i_c == history_col:
                    # fill empty sells with data before
                    days_history1 = days_history1.ffill()
                    try:
                        days_history = days_history1.iloc[:, 3:].astype(float).mean(axis=0)
                    except:
                        # if there is a column that does not mean. then try to use mean of other to fill in it's mean cell.
                        days_history1 = days_history1.T
                        mean = days_history1.iloc[3:-5, :].mean()
                        days_history1 = days_history1.fillna(mean)
                        days_history1 = days_history1.T
                        days_history = days_history1.iloc[:, 3:].astype(float).mean(axis=0)
                    break

        # calculate the mean of day date that is prepared before
        try:
            hours_history = hours_history.iloc[:, 3:].astype(float).mean(axis=0)
        except:
            i_c = 3
            for i in hours_history.columns:
                hours_history.iloc[:, i_c] = pd.to_numeric(hours_history.iloc[:, i_c], errors="coerce")
                i_c += 1
                if i_c == history_col:
                    hours_history = hours_history.ffill()
                    try:
                        hours_history = hours_history.iloc[:, 3:].astype(float).mean(axis=0)
                    except:
                        hours_history = hours_history.T
                        mean = hours_history.iloc[3:-5, :].mean()
                        hours_history = hours_history.fillna(mean)
                        hours_history = hours_history.T
                        hours_history = hours_history.iloc[:, 3:].astype(float).mean(axis=0)
                    break
        conn.commit()
        conn.close()
        if n == 1:
            try:
                return days_history, hours_history, month_mean, week_mean,today_max,today_min,highest_time,lowest_time
            except:
                month_mean = "-"
                week_mean = "-"
                return days_history, hours_history, month_mean, week_mean,today_max,today_min,highest_time,lowest_time
        else:
            return days_history, hours_history
    except Exception as e:
        print("history", e)
        traceback.print_exc()


def history2():
    try:
        conn = psycopg2.connect(
            dbname='mydb',
            user='myuser',
            password='377843',
            host='localhost',
            port='5432'
        )
        cur = conn.cursor()
    except Exception as e:
        print('postgre', e)
        conn = sqlite3.connect(database_gold)
        # Create a cursor object
        cur = conn.cursor()
        # Commit the changes
        conn.commit()
        # Execute a query to retrieve the data

    cur.execute("SELECT * FROM gold_data ORDER BY id DESC LIMIT 80000")  # Assuming 'id' is still your first column
    rows = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]

    history = pd.DataFrame(rows, columns=column_names)
    history = history.sort_values(by=history.columns[0], ascending=True)
    history_col = len(history.columns)
    days_history = history.groupby('id')

    try:
        formonthhistory = history.copy()
        formonthhistory.replace('', np.nan, inplace=True)
        formonthhistory = formonthhistory.bfill()
        # formonthhistory = formonthhistory.dropna()
        formonthhistory.index = formonthhistory['id']
        mon30 = formonthhistory.id.unique()[-30]
        mon0 = formonthhistory.id.unique()[-30]
        month = formonthhistory.loc[mon30:mon0]
        month_mean = month.iloc[:, 3:].astype(float).mean(axis=0)
        week7 = formonthhistory.id.unique()[-7]
        week0 = formonthhistory.id.unique()[-7]
        week = formonthhistory.loc[week7:week0]
        week_mean = week.iloc[:, 3:].astype(float).mean(axis=0)
    except Exception as e:
        print('monthly or weekly erorr, ', e)

    # separate today's data to get last hour data from it (without last 4 columns)
    group_day = history.id.unique()[-1]
    days_history = days_history.get_group(group_day)
    # days_history.iloc[:, -1:] = days_history.iloc[:, -1:].replace('', 55000000)
    days_history = days_history.replace('', np.nan)
    days_history.interpolate(method='linear', inplace=True)
    days_history.interpolate(method='ffill', inplace=True)
    days_history.interpolate(method='bfill', inplace=True)
    temp_mean = days_history.dropna()
    temp_mean = temp_mean.iloc[:, 3:-5].astype(float).mean(axis=0).mean()
    days_history.iloc[:, 3:-5] = days_history.iloc[:, 3:-5].replace('', temp_mean)
    hours_history = days_history.groupby('hour')
    last_two_hours = list(hours_history.groups.keys())
    last_two_hours=sorted([int(x) for x in last_two_hours])
    last_two_hours=last_two_hours[-2:]
    try:
        hours_history = hours_history.get_group(f"{int(last_two_hours[-1]):02d}")
    except:
        hours_history = hours_history.get_group(int(last_two_hours[-1]))    # select the group corresponding to the last hour
    try:
        days_history = days_history.iloc[:, 3:].astype(float).mean(axis=0)
    except:
        i_c = 3
        for i in hours_history.columns:
            days_history.iloc[:, i_c] = pd.to_numeric(days_history.iloc[:, i_c], errors="coerce")
            i_c += 1
            if i_c == history_col:
                days_history = days_history.ffill()
                try:
                    days_history = days_history.iloc[:, 3:].astype(float).mean(axis=0)
                except:
                    days_history = days_history.T
                    mean = days_history.iloc[3:-5, :].mean()
                    days_history = days_history.fillna(mean)
                    days_history = days_history.T
                    days_history = days_history.iloc[:, 3:].astype(float).mean(axis=0)
                break
    try:
        hours_history = hours_history.iloc[:, 3:].astype(float).mean(axis=0)
    except:
        # here if there is any problem with encoding or there is empty cells, try to handle it
        i_c = 3
        for i in hours_history.columns:
            hours_history.iloc[:, i_c] = pd.to_numeric(hours_history.iloc[:, i_c], errors="coerce")
            i_c += 1
            if i_c == history_col:
                # fill empty sells with data before
                hours_history = hours_history.ffill()
                try:
                    hours_history = hours_history.iloc[:, 3:].astype(float).mean(axis=0)
                except:
                    # if there is a column that does not mean. then try to use mean of other to fill in it's mean cell.
                    hours_history = hours_history.T
                    mean = hours_history.iloc[3:-5, :].mean()
                    hours_history = hours_history.fillna(mean)
                    hours_history = hours_history.T
                    hours_history = hours_history.iloc[:, 3:].astype(float).mean(axis=0)
                break
    conn.commit()
    conn.close()
    try:
        return days_history, hours_history, month_mean, week_mean
    except:
        month_mean = "-"
        week_mean = "-"
        return days_history, hours_history, month_mean, week_mean


def talasea(url_talasea, prices, names):
    try:
        names.append('talasea')
        response = requests.get(url_talasea,verify=False, timeout=10)
        data_talasea = json.loads(response.text)
        df_talasea = pd.json_normalize(data_talasea)
        talasea_price = df_talasea['price']
        talasea_price = pd.Series(talasea_price)
        talasea_price = talasea_price.iloc[-1]
        if len(str(talasea_price)) != 7:
            x_dif = 7 - len(str(talasea_price))
            x_dif = 10 ** x_dif
            talasea_price = int((int(talasea_price)) * x_dif)
        talasea_price = pd.Series(talasea_price)
        talasea_price = talasea_price.values.astype(int)
        prices.append(talasea_price)
    except:
        prices.append("")
        pass
    return prices, names


def miligold(url_miligold, prices, names):
    try:
        names.append('miligold')
        f = requests.get(url_miligold,verify=False, timeout=10)
        data = json.loads(f.text)
        df_miligold = pd.json_normalize(data)
        miligold_buy_price = pd.Series(df_miligold['price18'])
        miligold_buy_price = miligold_buy_price.iloc[-1]
        if len(str(miligold_buy_price)) != 7:
            x_dif = 7 - len(str(miligold_buy_price))
            x_dif = 10 ** x_dif
            miligold_buy_price = int((int(miligold_buy_price)) * x_dif)
        miligold_price = pd.Series(miligold_buy_price)
        miligold_price = miligold_price.values.astype(int)
        prices.append(miligold_price)
    except:
        prices.append("")
        pass
    return prices, names


def daric(url_daric, prices, names):
    try:
        names.append('daric')
        response = requests.get(url_daric,verify=False, timeout=10)
        data_daric = json.loads(response.text)
        data_daric = data_daric['lastFillOrders']
        df_daric = pd.json_normalize(data_daric)
        df_daric=df_daric['price'][0]
        if len(str(int(df_daric))) != 7:
            x_dif = 7 - len(str(df_daric))
            x_dif = 10 ** x_dif
            df_daric = int((int(df_daric)) * x_dif)
        daric_price = pd.Series(df_daric)
        daric_price = daric_price.values.astype(int)
        prices.append(daric_price)
    except:
        prices.append("")
        pass
    return prices, names


def tlyn(url_tlyn, prices, names):
    try:
        names.append('tlyn')
        response = requests.get(url_tlyn,verify=False, timeout=10)
        data_tlyn = response.json()
        tlyn_price = data_tlyn['prices'][0]['price']['sell']
        try:
            tlyn_price = (re.sub(r"\D", "", tlyn_price))
        except:
            pass
        if len(str(tlyn_price)) != 7:
            x_dif = 7 - len(str(tlyn_price))
            x_dif = 10 ** x_dif
            tlyn_price = int((int(tlyn_price)) * x_dif)
        tlyn_price = pd.Series(tlyn_price)
        tlyn_price = tlyn_price.values.astype(int)
        prices.append(tlyn_price)
    except:
        prices.append("")
        pass
    return prices, names


def talapp(url_talapp, prices, names):
    try:
        names.append('talapp')
        f = requests.get(url_talapp,verify=False, timeout=10)
        data = json.loads(f.text)
        df_talapp = pd.json_normalize(data)
        talapp_price =df_talapp['result.buy_gold'][0]
        if len(str(talapp_price)) != 7:
            x_dif = 7 - len(str(talapp_price))
            x_dif = 10 ** x_dif
            talapp_price = int((int(talapp_price)) * x_dif)
        talapp_price=pd.Series(talapp_price)
        talapp_price = talapp_price.values.astype(int)
        prices.append(talapp_price)
    except:
        prices.append("")
        pass
    return prices, names


def estjt(url_estjt, prices, names):
    try:
        names.append('estjt')
        response = requests.get(url_estjt, verify=False,timeout=20)
        soup = BeautifulSoup(response.content, 'html.parser')
        data_estjt = soup.select('.price')
        data_estjt = data_estjt[2].get_text()
        data_estjt = (re.sub(r"\D", "", data_estjt))
        if len(str(data_estjt)) != 7:
            x_dif = 7 - len(str(data_estjt))
            x_dif = 10 ** x_dif
            data_estjt = int((int(data_estjt)) * x_dif)
        estjt_price = pd.Series(data_estjt)
        estjt_price = estjt_price.values.astype(int)
        prices.append(estjt_price)
        dublicate_estjt = False
    except:
        if 'estjt_price_copy' in globals():
            global estjt_price_copy
            prices.append(int(estjt_price_copy))
            dublicate_estjt=True
        else:
            prices.append("")
            dublicate_estjt=True
    return prices, names,dublicate_estjt


async def ap(url_ap, prices, names):
    try:
        names.append('ap')
        async with websockets.connect(url_ap) as websocket:
            message = {
                "channel": "markets",
                "model": "all",
                "request": "SUBSCRIBE"
            }
            await websocket.send(json.dumps(message))
            json_data = await websocket.recv()
            data = json.loads(json_data)
            ap_price = next((market['open'] for market in data['message']['markets'] if market['market'] == 'goldirr'),
                            None)
            if len(str(ap_price)) != 7:
                x_dif = 7 - len(str(ap_price))
                x_dif = 10 ** x_dif
                ap_price = int((int(ap_price)) * x_dif)
            ap_price = pd.Series(ap_price)

            ap_price = ap_price.values.astype(int)
            prices.append(ap_price)
    except:
        prices.append("")
        pass
    return prices, names


def melli(url_melli, prices, names):
    try:
        names.append('melli')
        response = requests.get(url_melli,verify=False, timeout=10)
        data = response.json()
        melli_price = data['price_buy']
        if len(str(melli_price)) != 7:
            x_dif = 7 - len(str(melli_price))
            x_dif = 10 ** x_dif
            melli_price = int((int(melli_price)) * x_dif)
        melli_price = pd.Series(melli_price)
        melli_price = melli_price.values.astype(int)
        prices.append(melli_price)
    except:
        prices.append("")
        pass
    return prices, names


def tgju(url_tgju, prices, names):
    try:
        names.append('tgju')
        response = requests.get(url_tgju, verify=False, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
        info_prices = soup.select('.info-price')
        second_info_price = info_prices[3].get_text()
        second_info_price = (re.sub(r"\D", "", second_info_price))
        if len(str(second_info_price)) != 7:
            x_dif = 7 - len(str(second_info_price))
            x_dif = 10 ** x_dif
            second_info_price = int((int(second_info_price)) * x_dif)
        tgju_price = pd.Series(second_info_price)
        tgju_price = tgju_price.values.astype(int)
        prices.append(tgju_price)
    except:
        prices.append("")
        pass
    return prices, names


def wallgold(url_wallgold, prices, names):
    try:
        names.append('wallgold')
        response = requests.get(url_wallgold, verify=False,timeout=10)
        data = response.json()
        wallgold_price = data['result']['price']
        wallgold_price = (re.sub(r"\D", "", wallgold_price))
        if len(str(wallgold_price)) != 7:
            x_dif = 7 - len(str(wallgold_price))
            x_dif = 10 ** x_dif
            wallgold_price = int((int(wallgold_price)) * x_dif)
        wallgold_price = pd.Series(wallgold_price)
        wallgold_price = wallgold_price.values.astype(int)
        prices.append(wallgold_price)
    except:
        prices.append("")
        pass
    return prices, names


def technogold(url_technogold, prices, names):
    try:
        names.append('technogold')
        response = requests.get(url_technogold, verify=False,timeout=10)
        data = response.json()
        technogold_price = data['results']['price']
        if len(str(technogold_price)) != 7:
            x_dif = 7 - len(str(technogold_price))
            x_dif = 10 ** x_dif
            technogold_price = int((int(technogold_price)) * x_dif)
        technogold_price = pd.Series(technogold_price)
        technogold_price = technogold_price.values.astype(int)
        prices.append(technogold_price)
    except:
        prices.append("")
        pass
    return prices, names

def digikala(url_digikala, prices, names):
    try:
        names.append('digikala')
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-GB,en;q=0.9,fa-IR;q=0.8,fa;q=0.7,en-US;q=0.6",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            "upgrade-insecure-requests": "1"
        }

        cookies = {
            "tracker_glob_new": "8wLFBkW",
            "_ga": "GA1.1.1401555722.1719397470",
            "_ym_uid": "17255204807762074",
            "_ym_d": "1725520480",
            "_clck": "j3xugn%7C2%7Cfsh%7C0%7C1690",
            # ... (include all necessary cookies here)
            "ab_test_experiments": "%5B%22229ea1a233356b114984cf9fa2ecd3ff%22%2C%224905b18f64695e6dbfd739d20a4ae2c0%22%2C%22f0fd80107233fa604679779d7e121710%22%2C%2237136fdc21e0b782211ccac8c2d7be63%22%5D"
        }
        response = requests.get(url_digikala, headers=headers, cookies=cookies,verify=False,timeout=10)
        match = re.search(r'({.*})', response.text)
        json_str = match.group(1)
        data = json.loads(json_str)
        digikala_price = data['gold18']['price']
        if len(str(digikala_price)) != 7:
            x_dif = 7 - len(str(digikala_price))
            x_dif = 10 ** x_dif
            digikala_price = int((int(digikala_price)) * x_dif)
        digikala_price = pd.Series(digikala_price)
        digikala_price = digikala_price.values.astype(int)
        prices.append(digikala_price)
    except:
        prices.append("")
        pass
    return prices, names


def goldis(url_goldis, prices, names):
    try:
        names.append('goldis')
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "Referer": "https://shop.goldis.ir/login?mobile=09108302504",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }

        # Session with cookies (update this manually or login first)
        cookies = {
            "session": ".eJwNy7sNQjEMQNFdMoEd_1kGvTg2BRI0dIjdya3P_Y7H_fN-1mvcRgemgTDbupSsdE09WV8mNrfzohLnAg2b3KtgC1zosDvorNWOl7RWJVJu5GQS8UM2UCtwtJsr5Zww0yLXbkTsjgMDYPz-AS8mxw.aBoVXA.5PTMGRsGAesRmDA4zFr9wm_DSYg"
        }
        c = requests.get(url_goldis, verify=False, headers=headers, cookies=cookies, timeout=10)
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(c.content, 'html.parser')
        numbers = [span.text for span in soup.find_all("span", class_="fs12 goldColor text-right")]
        goldis_price = numbers[0]
        try:
            goldis_price = (re.sub(r"\D", "", goldis_price))
        except:
            pass
        if len(str(goldis_price)) != 7:
            x_dif = 7 - len(str(goldis_price))
            x_dif = 10 ** x_dif
            goldis_price = int((int(goldis_price)) * x_dif)
        goldis_price = pd.Series(goldis_price)
        goldis_price = goldis_price.values.astype(int)
        prices.append(goldis_price)
    except:
        prices.append("")
        pass
    return prices, names


def zarpad(url_zarpad, prices, names):
    try:
        names.append('zarpad')
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-CSRF-TOKEN": "vJu75jp2aJwGiFOXvhp6u6TywPR8hrpMcL5Ph9FR",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://app.zarpaad.com",
            "Referer": "https://app.zarpaad.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
        }

        cookies = {
            "XSRF-TOKEN": "eyJpdiI6IlhiTGZvWlV2Skp0MmM3VUFxZ1A3bVE9PSIsInZhbHVlIjoiLzhLWTk2dWhSb2JGWHRLaURhZ0RwR1FEUTZCVGdUclhJdDM5WFh0MmtaZDRySGg0ZC8rOE81cGVzT0lUdUFObUwrVXUvZElVVXhXelB5RGtpWklUNUV3VXBsa0ZpTVdUSGJGaUd3My95RWFHbTloT1hoWnhJTTlySnVtWUQwaWgiLCJtYWMiOiI1ZGQ0YTg1MjE0MWU0YTcwMWFlYzY5YjE4YWZmZDIxNzVkM2Y5NjI0ZDg4ZjI1YTg0OGNlM2FiNTljOWRiZTBiIiwidGFnIjoiIn0%3D",
            "zarpaad_session": "eyJpdiI6ImpsbHpRNWIyQksrL0pFbGMvUFNDcnc9PSIsInZhbHVlIjoiOThwL2l5V0tFalNIYWV1OXE3MHdtVXMyMGtwRllSbVBGa0lnWUFRUlRrZlFwbjkzOSt6THNkQmF1YWRhdEJnS2RKSTBjUElPVDJhR1gwSnlkMkdpa2ZIODFtUHM0UWYxQmFScGltdnBRNHBHbWo1NHlEYVVwSnROVVg3STdTUmMiLCJtYWMiOiJjYjUxZWU1ZmUzYmY0Y2U3YThmZmFlNTdiMjk2MDY0NWUzMzgyN2E5MWNmNzkzM2IzYmRlMjMwNzUwYTQ4MWY4IiwidGFnIjoiIn0%3D",
            # Add other cookies if necessary
        }

        # Replace these with the actual values used by the frontend
        data = {
            "id": "123",  # <- change this to your desired ID
            "type": "daily"  # <- or 'monthly', 'weekly', etc.
        }

        # Make the request
        response = requests.post(url_zarpad, headers=headers, cookies=cookies, data=data,verify=False,timeout=10)
        json_data = response.json()
        zarpad_price = json_data['data']['online_price']['gold_buy_rounded']
        try:
            zarpad_price = (re.sub(r"\D", "", zarpad_price))
        except:
            pass
        if len(str(zarpad_price)) != 7:
            x_dif = 7 - len(str(zarpad_price))
            x_dif = 10 ** x_dif
            zarpad_price = int((int(zarpad_price)) * x_dif)
        zarpad_price = pd.Series(zarpad_price)
        zarpad_price = zarpad_price.values.astype(int)
        prices.append(zarpad_price)
    except:
        prices.append("")
        pass
    return prices, names


def goldika(url_goldika, prices, names):
    try:
        names.append('goldika')
        f = requests.get(url_goldika, timeout=10)
        data_goldika = json.loads(f.text)
        df_goldika = pd.json_normalize(data_goldika)
        goldika_buy_price = df_goldika['data.price.buy']
        goldika_buy_price = goldika_buy_price.iloc[-1]
        if len(str(goldika_buy_price)) != 7:
            x_dif = 7 - len(str(goldika_buy_price))
            x_dif = 10 ** x_dif
            goldika_price = int((int(goldika_buy_price)) * x_dif)
        goldika_price = pd.Series(goldika_price)

        goldika_price = goldika_price.values.astype(int)
        prices.append(goldika_price)
    except:
        prices.append("")
        pass

    return prices, names


def bazaretala(url_bazaretala, prices, names):
    try:
        names.append('bazaretala')
        response = requests.get(url_bazaretala,verify=False, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
        data_bazaretala = soup.find('span', attrs={
            'class': 'goldBuyPrice'}).text  # replace with the actual tag name and class name
        data_bazaretala = (re.sub(r"\D", "", data_bazaretala))
        if len(str(data_bazaretala)) != 7:
            x_dif = 7 - len(str(data_bazaretala))
            x_dif = 10 ** x_dif
            data_bazaretala = int((int(data_bazaretala)) * x_dif)
        bazaretala_price = pd.Series(data_bazaretala)
        # bazaretala_buy_price=(re.sub("\D", "",bazaretala_buy_price))
        bazaretala_price.name = 'bazaretala_price'
        bazaretala_price = bazaretala_price.values.astype(int)
        prices.append(bazaretala_price)
    except:
        prices.append("")
        pass
    return prices, names


def ounce(url_ounce, prices, names):
    try:
        names.append('ounce')
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0"
        }
        data = requests.get(url_ounce, headers=headers,verify=False, timeout=10).json()
        ounce = data["items"][0]["xauPrice"], data["items"][0]["pcXau"]
        ounce = str(np.float64(ounce[0]))
        ounce_price = (re.sub(r"\D", "", ounce))
        if len(str(ounce_price)) > 4:
            x_dif = 4 - len(str(ounce_price))
            x_dif = 10 ** x_dif
            ounce_price = np.float64((np.float64(ounce_price)) * x_dif)
            ounce_price = round(ounce_price, 2)
        ounce_price = pd.Series(ounce_price)
        ounce_price = ounce_price.values.astype(float)
        prices.append(ounce_price)
        dublicate_ounce = False
    except:
        if 'ounce_price_copy' in globals():
            global ounce_price_copy
            prices.append(np.float64(ounce_price_copy))
            ounce_price = np.float64(ounce_price_copy)
            ounce_price=[ounce_price]
            dublicate_ounce = True
        else:
            prices.append("")
            ounce_price=''
            dublicate_ounce = True
        pass
    return prices,  names, ounce_price, dublicate_ounce


def dollar(url_wallex,url_dollar, prices, ounce_price, names):
    try:
        names.append('dollar')
        names.append('dollar_based')
        try:
            response = requests.get(url_dollar,verify=False, timeout=15)
            data = response.json()
            dollar_price = data['stats']['usdt-rls']['latest']
        except:
            response = requests.get(url_wallex, verify=False, timeout=15)
            data = response.json()
            dollar_price = int(float(data['result']['latestTrades'][0]['price']))
        if len(str(dollar_price)) < 6:
            dollar_price = int((int(dollar_price)) * 10)

        dollar_price = int(dollar_price)
        prices.append(dollar_price)
        try:
            dollar_based_price = (np.float64(np.int32(dollar_price)) * np.float64(np.int32(ounce_price[0])) * 0.075 * 0.999) / 31.1034768
            dollar_based_price_copy = dollar_based_price
            prices.append(dollar_based_price)

        except Exception as e:
            print('dollar func, ',e)
            dollar_based_price_copy = ""
            prices.append("")
            pass
    except:
        dollar_based_price_copy = ""
        prices.append("")
        prices.append("")
        pass
    return prices, names, dollar_based_price_copy


def coin(url_estjt, prices, names):
    try:
        names.append('coin')
        response = requests.get(url_estjt,verify=False, timeout=20)
        soup = BeautifulSoup(response.content, 'html.parser')
        data_coin = soup.select('.price')
        lenn=len(data_coin)-5
        data_coin = data_coin[lenn].get_text()
        data_coin = (re.sub(r"\D", "", data_coin))
        if len(str(data_coin)) < 8:
            x_dif = 8 - len(str(data_coin))
            x_dif = 10 ** x_dif
            data_coin = int((int(data_coin)) * x_dif)
        coin_price = pd.Series(data_coin)
        coin_price = coin_price.values.astype(int)
        prices.append(coin_price)
        dublicate_coin = False
    except:
        if 'coin_price_copy' in globals():
            global coin_price_copy
            prices.append(int(coin_price_copy))
            dublicate_coin = True
        else:
            prices.append("")
            dublicate_coin = True

        pass
    return prices, names, dublicate_coin


def situ(price1, price2):
    try:
        if round((price1 - price2)*100/price1,2) >=0.01:
            situation = emoji.emojize(':green_circle:') + " "
        elif round((price1 - price2)*100/price1,2) <=-0.01:
            situation = emoji.emojize(':red_circle:')
        else:
            situation = emoji.emojize(':radio_button:') + " "
    except:
        situation = emoji.emojize(':radio_button:') + " "
    return situation


def getting_data():
    try:
        prices = []
        names = []
        url_miligold = "https://milli.gold/api/v1/public/milli-price/detail"
        url_daric = "https://apie.daric.gold/api/dashboard/3ac5640c-58ec-4e5d-bc92-54504512324c/fills"
        url_talapp = "https://panel.talapp.ir/api/last_price"
        url_bazaretala = "https://bazaretala.com/melted-gold-bar"
        url_talasea = "https://Api.talasea.ir/api/market/getGoldprice"
        url_wallex = "https://api.wallex.ir/v1/trades?symbol=USDTTMN"
        url_tlyn = "https://price.tlyn.ir/api/v1/price"
        url_goldika = "https://goldika.ir/api/public/price"
        url_tgju = "https://www.tgju.org/profile/geram18"
        url_estjt = "https://www.estjt.ir/"
        url_ounce = "https://data-asg.goldprice.org/dbXRates/USD"
        url_dollar = "https://apiv2.nobitex.ir/market/stats?srcCurrency=usdt&dstCurrency=rls"
        url_ap = "wss://cryptian.com/ws"
        url_melli = 'https://melligold.com/buying/selling/price/XAU18/'
        url_wallgold='https://api.wallgold.ir/api/v1/price?side=buy&symbol=GLD_18C_750TMN'
        url_technogold ='https://api2.technogold.gold/customer/tradeables/only-price/1'
        url_coin = 'https://www.tgju.org/profile/sekee'
        url_goldis= "https://shop.goldis.ir/home"
        url_zarpad="https://app.zarpaad.com/ajax/GetValues"
        url_digikala = ('https://api.digikala.com/non-inventory/v1/prices/')
        try:
            prices, names = talasea(url_talasea, prices, names)
            prices, names = miligold(url_miligold, prices, names)
            prices, names = tlyn(url_tlyn, prices, names)
            prices, names = daric(url_daric, prices, names)
            prices, names = talapp(url_talapp, prices, names)
            prices, names , dublicate_estjt = estjt(url_estjt, prices, names)
            prices, names = asyncio.run(ap(url_ap, prices, names))
            prices, names = tgju(url_tgju, prices, names)
            prices, names = melli(url_melli, prices, names)
            prices, names = wallgold(url_wallgold, prices, names)
            prices, names = technogold(url_technogold, prices, names)
            prices, names = digikala(url_digikala, prices, names)
            prices, names = goldis(url_goldis, prices, names)
            prices, names = zarpad(url_zarpad, prices, names)
            prices, names = goldika(url_goldika, prices, names)
            prices, names = bazaretala(url_bazaretala, prices, names)
            prices, names , ounce_price, dublicate_ounce = ounce(url_ounce, prices, names)
            prices, names , dollar_based_price_copy = dollar(url_wallex,url_dollar, prices, ounce_price, names)
            prices, names , dublicate_coin = coin(url_estjt, prices, names)
            prices = pd.Series(prices)
            prices.index = names
        except Exception as e:
            print('price, names, ',e)
        try:
            prices = prices.replace('', 0)
            prices = prices.astype(float)
            prices = prices.replace(0, "")
            price=pd.DataFrame(prices)
        except:
            print('ERROR 2')

        return prices.T, dollar_based_price_copy,dublicate_estjt,dublicate_ounce,dublicate_coin
    except:
        prices = pd.DataFrame(prices)
        print('ERROR 3')
        return prices.T, dollar_based_price_copy,dublicate_estjt,dublicate_ounce,dublicate_coin

def sec_func(prices, days_history_24):
    rank_list = []
    rank_dif_list = []
    rank_name_list = []
    situ_list = []

    try:
        try:
            talasea_situ = situ(prices.talasea, days_history_24.talasea)
            talasea_dif = np.round(((prices.talasea - days_history_24.talasea) / days_history_24.talasea) * 100, 2)
            if abs(talasea_dif) < 0.01:
                talasea_dif = "0.00"
        except:
            talasea_situ = emoji.emojize(':radio_button:') + " "
            talasea_dif = ""
        rank_list.append(prices.talasea)
        rank_name_list.append('طلاسی')
        situ_list.append(talasea_situ)
        rank_dif_list.append(talasea_dif)
    except:
        pass

    try:
        try:
            miligold_situ = situ(prices.miligold, days_history_24.miligold)
            miligold_dif = np.round(((prices.miligold - days_history_24.miligold) / days_history_24.miligold) * 100, 2)
            if abs(miligold_dif) < 0.01:
                miligold_dif = "0.00"
        except:
            miligold_situ = emoji.emojize(':radio_button:') + " "
            miligold_dif = ""
        rank_list.append(prices.miligold)
        rank_name_list.append('میلی گلد')
        situ_list.append(miligold_situ)
        rank_dif_list.append(miligold_dif)
    except:
        pass

    try:
        try:
            tlyn_situ = situ(prices.tlyn, days_history_24.tlyn)
            tlyn_dif = np.round(((prices.tlyn - days_history_24.tlyn) / days_history_24.tlyn) * 100, 2)
            if abs(tlyn_dif) < 0.01:
                tlyn_dif = "0.00"
        except:
            tlyn_situ = emoji.emojize(':radio_button:') + " "
            tlyn_dif = ""
        rank_list.append(prices.tlyn)
        rank_name_list.append('طلاین')
        situ_list.append(tlyn_situ)
        rank_dif_list.append(tlyn_dif)
    except:
        pass

    try:
        try:
            daric_situ = situ(prices.daric, days_history_24.daric)
            daric_dif = np.round(((prices.daric - days_history_24.daric) / days_history_24.daric) * 100, 2)
            if abs(daric_dif) < 0.01:
                daric_dif = "0.00"
        except:
            daric_situ = emoji.emojize(':radio_button:') + " "
            daric_dif = ""
        rank_list.append(prices.daric)
        rank_name_list.append('داریک')
        situ_list.append(daric_situ)
        rank_dif_list.append(daric_dif)
    except:
        pass

    try:
        try:
            talapp_situ = situ(prices.talapp, days_history_24.talapp)
            talapp_dif = np.round(((prices.talapp - days_history_24.talapp) / days_history_24.talapp) * 100, 2)
            if abs(talapp_dif) < 0.01:
                talapp_dif = "0.00"
        except:
            talapp_situ = emoji.emojize(':radio_button:') + " "
            talapp_dif = ""
        rank_list.append(prices.talapp)
        rank_name_list.append('طلاپ')
        situ_list.append(talapp_situ)
        rank_dif_list.append(talapp_dif)
    except:
        pass

    try:
        try:
            estjt_situ = situ(prices.estjt, days_history_24.estjt)
            estjt_dif = np.round(((prices.estjt - days_history_24.estjt) / days_history_24.estjt) * 100, 2)
            if abs(estjt_dif) < 0.01:
                estjt_dif = "0.00"
        except:
            estjt_situ = emoji.emojize(':radio_button:') + " "
            estjt_dif = ""

        prices.estjt = int(prices.estjt)
        estjt_price_copy = prices.estjt
        estjt_price = "{:,}".format(int(prices.estjt))
    except:
        estjt_price =""
        estjt_price_copy=""
        pass

    try:
        try:
            ap_situ = situ(prices.ap, days_history_24.ap)
            ap_dif = np.round(((prices.ap - days_history_24.ap) / days_history_24.ap) * 100, 2)
            if abs(ap_dif) < 0.01:
                ap_dif = "0.00"
        except:
            ap_situ = emoji.emojize(':radio_button:') + " "
            ap_dif = ""
        rank_list.append(prices.ap)
        rank_name_list.append('آپ')
        situ_list.append(ap_situ)
        rank_dif_list.append(ap_dif)
    except:
        pass

    try:
        try:
            tgju_situ = situ(prices.tgju, days_history_24.tgju)
            tgju_dif = np.round(((prices.tgju - days_history_24.tgju) / days_history_24.tgju) * 100, 2)
            if abs(tgju_dif) < 0.01:
                tgju_dif = "0.00"
        except:
            tgju_situ = emoji.emojize(':radio_button:') + " "
            tgju_dif = ""
        # rank_list.append(prices.tgju)
        # rank_name_list.append('تی جی')
        # situ_list.append(tgju_situ)
        # rank_dif_list.append(tgju_dif)
    except:
        pass
    try:
        try:
            melli_situ = situ(prices.melli, days_history_24.melli)
            melli_dif = np.round(((prices.melli - days_history_24.melli) / days_history_24.melli) * 100, 2)
            if abs(melli_dif) < 0.01:
                melli_dif = "0.00"
        except:
            melli_situ = emoji.emojize(':radio_button:') + " "
            melli_dif = ""
        rank_list.append(prices.melli)
        rank_name_list.append('ملّی گلد')
        situ_list.append(melli_situ)
        rank_dif_list.append(melli_dif)
    except:
        pass

    try:
        try:
            wallgold_situ = situ(prices.wallgold, days_history_24.wallgold)
            wallgold_dif = np.round(((prices.wallgold - days_history_24.wallgold) / days_history_24.wallgold) * 100, 2)
            if abs(wallgold_dif) < 0.01:
                wallgold_dif = "0.00"
        except:
            wallgold_situ = emoji.emojize(':radio_button:') + " "
            wallgold_dif = ""
        rank_list.append(prices.wallgold)
        rank_name_list.append('وال‌گلد')
        situ_list.append(wallgold_situ)
        rank_dif_list.append(wallgold_dif)
    except:
        pass


    try:
        try:
            technogold_situ = situ(prices.technogold, days_history_24.technogold)
            technogold_dif = np.round(((prices.technogold - days_history_24.technogold) / days_history_24.technogold) * 100, 2)
            if abs(technogold_dif) < 0.01:
                technogold_dif = "0.00"
        except:
            technogold_situ = emoji.emojize(':radio_button:') + " "
            technogold_dif = ""
        rank_list.append(prices.technogold)
        rank_name_list.append('تکنوگلد')
        situ_list.append(technogold_situ)
        rank_dif_list.append(technogold_dif)
    except:
        pass


    try:
        try:
            digikala_situ = situ(prices.digikala, days_history_24.digikala)
            digikala_dif = np.round(((prices.digikala - days_history_24.digikala) / days_history_24.digikala) * 100, 2)
            if abs(digikala_dif) < 0.01:
                digikala_dif = "0.00"
        except:
            digikala_situ = emoji.emojize(':radio_button:') + " "
            digikala_dif = ""
        rank_list.append(prices.digikala)
        rank_name_list.append('دیجیکالا')
        situ_list.append(digikala_situ)
        rank_dif_list.append(digikala_dif)
    except:
        pass


    try:
        try:
            goldis_situ = situ(prices.goldis, days_history_24.goldis)
            goldis_dif = np.round(((prices.goldis- days_history_24.goldis) / days_history_24.goldis) * 100, 2)
            if abs(goldis_dif) < 0.01:
                goldis_dif = "0.00"
        except:
            goldis_situ = emoji.emojize(':radio_button:') + " "
            goldis_dif = ""
        rank_list.append(prices.goldis)
        rank_name_list.append('گلدیس')
        situ_list.append(goldis_situ)
        rank_dif_list.append(goldis_dif)
    except:
        pass

    try:
        try:
            zarpad_situ = situ(prices.zarpad, days_history_24.zarpad)
            zarpad_dif = np.round(((prices.zarpad - days_history_24.zarpad) / days_history_24.zarpad) * 100, 2)
            if abs(zarpad_dif) < 0.01:
                zarpad_dif = "0.00"
        except:
            zarpad_situ = emoji.emojize(':radio_button:') + " "
            zarpad_dif = ""
        rank_list.append(prices.zarpad)
        rank_name_list.append('زرپاد')
        situ_list.append(zarpad_situ)
        rank_dif_list.append(zarpad_dif)
    except:
        pass


    try:
        try:
            goldika_situ = situ(prices.goldika, days_history_24.goldika)
            goldika_dif = np.round(((prices.goldika - days_history_24.goldika) / days_history_24.goldika) * 100, 2)
            if abs(goldika_dif) < 0.01:
                goldika_dif = "0.00"
        except:
            goldika_situ = emoji.emojize(':radio_button:') + " "
            goldika_dif = ""
        rank_list.append(prices.goldika)
        rank_name_list.append('گلدیکا')
        situ_list.append(goldika_situ)
        rank_dif_list.append(goldika_dif)
    except:
        pass

    try:
        try:
            bazaretala_situ = situ(prices.bazaretala, days_history_24.bazaretala)
            bazaretala_dif = np.round(
                ((prices.bazaretala - days_history_24.bazaretala) / days_history_24.bazaretala) * 100, 2)
            if abs(bazaretala_dif) < 0.01:
                bazaretala_dif = "0.00"
        except:
            bazaretala_situ = emoji.emojize(':radio_button:') + " "
            bazaretala_dif = ""
        # rank_list.append(prices.bazaretala)
        # rank_name_list.append('بازارطلا')
        # situ_list.append(bazaretala_situ)
        # rank_dif_list.append(bazaretala_dif)
    except:
        pass

    try:
        try:
            ounce_situ = situ(prices.ounce, days_history_24.ounce)
            ounce_dif = np.round(((prices.ounce - days_history_24.ounce) / days_history_24.ounce) * 100, 2)
            if abs(ounce_dif)<0.01:
                ounce_dif="0.00"
        except:
            ounce_situ = emoji.emojize(':radio_button:') + " "
            ounce_dif = ""

        prices.ounce = float(prices.ounce)
        ounce_price_copy = prices.ounce
        ounce_price = "{:,.2f}".format(np.float64(prices.ounce))
    except:
        ounce_price_copy = ""
        ounce_price=""
        pass

    try:
        dollar_situ = situ(prices.dollar, days_history_24.dollar)

        dollar_dif = np.round(((prices.dollar - days_history_24.dollar) / days_history_24.dollar) * 100, 2)
        if np.isnan(dollar_dif):
            dollar_dif = "0.00"
        try:
            if abs(dollar_dif) < 0.01:
                dollar_dif = "0.00"
        except:
            pass
        try:
            dollar_based_situ = situ(prices.dollar_based, days_history_24.dollar_based)
            dollar_based_dif = np.round(
                ((prices.dollar_based - days_history_24.dollar_based) / days_history_24.dollar_based) * 100, 2)
            if abs(dollar_based_dif)<0.01:
                dollar_based_dif="0.00"
        except:
            dollar_based_situ = emoji.emojize(':radio_button:') + " "
            dollar_based_dif = ""
    except:
        dollar_situ = emoji.emojize(':radio_button:') + " "
        dollar_dif = "0.00"
        dollar_based_situ = emoji.emojize(':radio_button:') + " "
        dollar_based_dif = "0.00"
    try:
        dollar_price = int(prices.dollar) / 10
        dollar_price_copy=dollar_price
        dollar_price = "{:,}".format(int(dollar_price))
        try:
            dollar_based_price = int(prices.dollar_based)
            dollar_based_price = "{:,}".format(int(dollar_based_price))
        except:
            dollar_based_price =""
            pass
    except:
        dollar_price = ""
        dollar_price_copy = ""
        dollar_based_price = ""
        pass

    try:
        try:
            coin_situ = situ(prices.coin, days_history_24.coin)
            coin_dif = np.round(((prices.coin - days_history_24.coin) / days_history_24.coin) * 100, 2)
            if abs(coin_dif)<0.01:
                coin_dif="0.00"
        except:
            coin_situ = emoji.emojize(':radio_button:') + " "
            coin_dif = ""

        prices_coin = int(prices.coin)
        coin_price_copy = prices_coin
        coin_price = "{:,}".format(int(prices.coin))
    except:
        coin_price_copy=""
        coin_price=""
        pass

    # rankig
    try:
        rank_list = pd.Series(rank_list)
        mess_df = pd.DataFrame(rank_list.values, index=rank_name_list)
        mess_df.columns = ['rank_list']
        mess_df["situ"] = situ_list
        mess_df["dif"] = rank_dif_list
        mess_df=mess_df.replace("",np.nan).dropna()
        mess_df = mess_df.sort_values(by=['rank_list'])
        rank_list = list(mess_df["rank_list"])
        rank_name_list = list(mess_df.index)
        situ_list = list(mess_df["situ"])
        rank_dif_list = list(mess_df["dif"])
    except Exception as e:
        print("ERROR 3.5,", e)
        print(rank_list)
        print(rank_name_list)

    # add comma to data
    pos = 0
    for price in rank_list:
        try:
            price = "{:,}".format(int(price))
            rank_list[pos] = price
            pos += 1
        except:
            print('ERROR 4')

    return rank_list, rank_name_list, situ_list, rank_dif_list, prices, estjt_price, estjt_situ, estjt_dif, coin_price, coin_situ, coin_dif, ounce_price, ounce_situ, ounce_dif, dollar_price, dollar_situ, dollar_dif, dollar_based_price, dollar_based_situ, dollar_based_dif,coin_price_copy,dollar_price_copy,ounce_price_copy,estjt_price_copy,coin_price_copy


while True:
    try:
        # set start time to shift it at any critical point
        if n == 0:
            timeStop_zero = datetime.now()
            time_start = timeStop_zero.timestamp()
            n += 1
        print("Try")
        # set time intervals
        sleep = 60 * n



        prices, dollar_based_price_copy, dublicate_estjt, dublicate_ounce, dublicate_coin=getting_data()# Set timeout to 5 seconds

        # getting time date to store
        try:
            time_next, jalali_date, Times_hour, Times_min, df_jalalidate,day = time_function(n, time_start, sleep)
        except Exception as e:
            print(e)
            print("ERROR 6")
        # setting critical times point to change comparable data
        try:
            if n == 1:
                days_history_24, hours_history_1, month_mean, week_mean = "", "", "", ""
                pos_last_growth_24_ounce_price = 0
                neg_last_growth_24_ounce_price = 0
                pos_last_growth_24=0
                pos_last_growth_1 = 0
                neg_last_growth_24 = 0
                neg_last_growth_1 = 0
                try:
                    days_history_24, hours_history_1, month_mean, week_mean,today_max,today_min,highest_time,lowest_time = history(n)
                    month_mean_copy = month_mean.copy()
                    week_mean_copy = week_mean.copy()
                    days_history_24_copy = days_history_24.copy()
                    hours_history_1_copy = hours_history_1.copy()
                    Times_hour_last = Times_hour
                    jalali_date_last = day
                except:
                    Times_hour_last = Times_hour
                    jalali_date_last =  day
            if Times_hour != Times_hour_last:
                print ('hour changed')
                print ('day, ', day)
                print('jalali_date_last, ', jalali_date_last)
                hour_change=True


                # when hours change
                days_history_24, hours_history_1 = "", ""
                days_history_24, hours_history_1 = history(n)
                month_mean_copy = month_mean.copy()
                week_mean_copy = week_mean.copy()
                days_history_24_copy = days_history_24.copy()
                hours_history_1_copy = hours_history_1.copy()
                Times_hour_last = Times_hour
                try:
                    if Times_hour == '01':
                        repo_path = os.getcwd()
                        file_to_add = repo_path + '\\' + 'gold_price_data.db'

                        commit_message = f'Add file to repository at {df_jalalidate[-1]}'
                        os.chdir(repo_path)
                        # subprocess.run(['git', 'add', file_to_add])
                        subprocess.run(['git', 'pull'], timeout=60, check=True)
                        subprocess.run(['git', 'add', "--all"], timeout=60, check=True)
                        subprocess.run(['git', 'add', "--all"], timeout=60, check=True)
                        subprocess.run(['git', 'commit', '-m', commit_message], timeout=60, check=True)
                        subprocess.run(['git', 'push'], timeout=60, check=True)
                except Exception as e:
                    print('github error, ',e)

                # The use of checking the time before checking the day is that, getting the last hour information, when the day doesn't change, is on different based than when it does.
                if jalali_date_last !=  day:  #when days change
                    print('day changed')
                    days_history_24, hours_history_1, month_mean, week_mean = "", "", "", ""
                    days_history_24, hours_history_1, month_mean, week_mean = history2()
                    month_mean_copy = month_mean.copy()
                    week_mean_copy = week_mean.copy()
                    days_history_24_copy = days_history_24.copy()
                    hours_history_1_copy = hours_history_1.copy()
                    day_change = True
                    jalali_date_last =  day
            else:
                print()
        except:
            pass





        try:
            week_mean = week_mean.replace('', np.nan)
            week_mean = week_mean.dropna()
            month_mean = month_mean.replace('', np.nan)
            month_mean = month_mean.dropna()
            days_history_week_mean = week_mean.iloc[:-5].mean()
            days_history_month_mean = month_mean.iloc[:-5].mean()
        except:
            print('ERROR 7')

        try:
            days_history_24 = days_history_24.replace('', np.nan)
            days_history_24 = days_history_24.dropna()
            hours_history_1 = hours_history_1.replace('', np.nan)
            hours_history_1 = hours_history_1.dropna()
            days_history_24mean = days_history_24.iloc[:-5].mean()
            hours_history_1mean = hours_history_1.iloc[:-5].mean()
        except:
            print('ERROR 8')



        if n==1:
            prices3 = prices.copy()
            n2=0
        try:
            price_check=pd.concat([prices,prices3],axis=1).T
            price_check=price_check.replace("",np.nan).dropna(axis=1).pct_change().iloc[-1] * 100
            price_check=price_check[abs(price_check) >= 20]
            for i in price_check.index:
                price_drop=i
                n2=n+30
            if n2 > n:
                prices[f'{price_drop}'] = ""
                week_mean[f"{colu}"] = ""
                month_mean[f"{colu}"] = ""
                days_history_24[f"{colu}"] = ""
                hours_history_1[f"{colu}"] = ""
        except Exception as e:
            print('price_check, ',e)
        prices3=prices.copy()



        try:
            prices2 = prices.copy()
            prices2 = prices2.iloc[:-4].replace('', np.nan)
            prices2_5=prices.iloc[-4:].replace('', np.nan)
            prices2 = prices2.dropna()
            now_mean_zero = prices2.mean()
            print(now_mean_zero)
        except:
            print('ERROR 5')
        try:

            try:
                for colu in prices2.index:
                    try:
                        if ((abs((prices2[f"{colu}"] - now_mean_zero) / now_mean_zero * 100) > primary_alarm_treshold)
                            or (abs((week_mean_copy[f"{colu}"] - days_history_week_mean) / days_history_week_mean * 100) > primary_alarm_treshold)
                            or (abs((month_mean_copy[f"{colu}"] - days_history_month_mean) / days_history_month_mean * 100) > primary_alarm_treshold)
                            or (abs((days_history_24_copy[f"{colu}"] - days_history_24mean) / days_history_24mean * 100) > primary_alarm_treshold)
                            or (abs((hours_history_1_copy[f"{colu}"] - hours_history_1mean) / hours_history_1mean * 100) > primary_alarm_treshold)):
                            prices2[f"{colu}"] = ""
                            week_mean[f"{colu}"] = ""
                            month_mean[f"{colu}"] = ""
                            days_history_24[f"{colu}"] = ""
                            hours_history_1[f"{colu}"] = ""
                    except:
                        pass

            except:
                pass
            prices2 = prices2.replace('', np.nan)
            prices2 = prices2.dropna()
            now_mean_zero = prices2.mean()

            try:
                for colu in prices2.index:
                    try:
                        if ((abs((prices2[f"{colu}"] - now_mean_zero) / now_mean_zero * 100) > secondary_alarm_treshold)
                            or (abs((week_mean_copy[f"{colu}"] - days_history_week_mean) / days_history_week_mean * 100) > secondary_alarm_treshold)
                            or (abs((month_mean_copy[f"{colu}"] - days_history_month_mean) / days_history_month_mean * 100) > secondary_alarm_treshold)
                            or (abs((days_history_24_copy[f"{colu}"] - days_history_24mean) / days_history_24mean * 100) > secondary_alarm_treshold)
                            or (abs((hours_history_1_copy[f"{colu}"] - hours_history_1mean) / hours_history_1mean * 100) > secondary_alarm_treshold)):
                            prices2[f"{colu}"] = ""
                            week_mean[f"{colu}"] = ""
                            month_mean[f"{colu}"] = ""
                            days_history_24[f"{colu}"] = ""
                            hours_history_1[f"{colu}"] = ""
                    except:
                        pass
            except:
                pass
            prices2 = prices2.replace('', np.nan)
            prices2 = prices2.dropna()
            now_mean_zero = prices2.mean()
        except Exception as e:
            prices2 = ""
            print('after ERROR 5', e)

        price2=pd.concat([prices2,prices2_5],axis=0)



        try:
            week_mean = week_mean.replace('', np.nan)
            week_mean = week_mean.dropna()
            month_mean = month_mean.replace('', np.nan)
            month_mean = month_mean.dropna()
            days_history_week_mean = week_mean.iloc[:-5].mean()
            days_history_month_mean = month_mean.iloc[:-5].mean()
        except:
            print('ERROR 7')

        try:
            days_history_24 = days_history_24.replace('', np.nan)
            days_history_24 = days_history_24.dropna()
            hours_history_1 = hours_history_1.replace('', np.nan)
            hours_history_1 = hours_history_1.dropna()
            days_history_24mean = days_history_24.iloc[:-5].mean()
            hours_history_1mean = hours_history_1.iloc[:-5].mean()
        except:
            print('ERROR 8')



        try:
            prices4=prices.copy()
            for colu in prices4.iloc[:-4].index:
                try:

                    if ((abs((prices4[f"{colu}"]-now_mean_zero)/now_mean_zero*100)>alarm_treshold )
                    or (abs((week_mean_copy[f"{colu}"] - days_history_week_mean) / days_history_week_mean * 100) > alarm_treshold)
                    or (abs((month_mean_copy[f"{colu}"] - days_history_month_mean) / days_history_month_mean * 100) > alarm_treshold)
                    or (abs((days_history_24_copy[f"{colu}"] - days_history_24mean) / days_history_24mean * 100) > alarm_treshold)
                    or (abs((hours_history_1_copy[f"{colu}"] - hours_history_1mean) / hours_history_1mean * 100) > alarm_treshold)):
                        # if colu!='estjt':
                            prices4[f"{colu}"]=""
                            week_mean[f"{colu}"] = ""
                            month_mean[f"{colu}"] = ""
                            days_history_24[f"{colu}"] = ""
                            hours_history_1[f"{colu}"] = ""
                except:
                    prices4[f"{colu}"] = ""
                    week_mean[f"{colu}"] = ""
                    month_mean[f"{colu}"] = ""
                    days_history_24[f"{colu}"] = ""
                    hours_history_1[f"{colu}"] = ""
                    pass
            prices2 = prices4.copy()
            prices2 = prices2.iloc[:-4]
            prices2 = prices2.replace('', np.nan)
            prices2 = prices2.dropna()
            now_mean = prices2.mean()
        except:
            now_mean=now_mean_zero
            pass

        try:
            week_mean = week_mean.replace('', np.nan)
            week_mean = week_mean.dropna()
            month_mean = month_mean.replace('', np.nan)
            month_mean = month_mean.dropna()
            days_history_week_mean = week_mean.iloc[:-5].mean()
            days_history_month_mean = month_mean.iloc[:-5].mean()
        except:
            print('ERROR 7')

        try:
            days_history_24 = days_history_24.replace('', np.nan)
            days_history_24 = days_history_24.dropna()
            hours_history_1 = hours_history_1.replace('', np.nan)
            hours_history_1 = hours_history_1.dropna()
            days_history_24mean = days_history_24.iloc[:-5].mean()
            hours_history_1mean = hours_history_1.iloc[:-5].mean()
        except:
            print('ERROR 8')
        try:
            rank_list, rank_name_list, situ_list, rank_dif_list, prices, estjt_price, estjt_situ, estjt_dif, coin_price, coin_situ, coin_dif, ounce_price, ounce_situ, ounce_dif, dollar_price, dollar_situ, dollar_dif, dollar_based_price, dollar_based_situ, dollar_based_dif,coin_price_copy,dollar_price_copy,ounce_price_copy,estjt_price_copy,coin_price_copy = sec_func(prices, days_history_24_copy)
        except Exception as e:
            print(e)
            traceback.print_exc()

            print('ERROR Baqerzadeh:)')
        try:
            if n==1:
                try:
                    highest_price_copy=today_max
                    lowest_price_copy=today_min
                    highest_time_copy = highest_time
                    lowest_time_copy = lowest_time
                except:
                    highest_price_copy=now_mean
                    lowest_price_copy=now_mean
                    highest_time_copy = Times_min
                    lowest_time_copy = Times_min
            if day_change==True:
                highest_price_copy = now_mean
                lowest_price_copy = now_mean
                highest_time_copy = Times_min
                lowest_time_copy = Times_min
            if now_mean>highest_price_copy:
                highest_price_copy=now_mean
                highest_time_copy = Times_min
            if now_mean < lowest_price_copy:
                lowest_price_copy = now_mean
                lowest_time_copy = Times_min
            lowest_price="{:,}".format(int(lowest_price_copy))
            highest_price="{:,}".format(int(highest_price_copy))
            highest_time=highest_time_copy
            lowest_time=lowest_time_copy
        except:
            try:
                lowest_price = "{:,}".format(int(lowest_price_copy / 10))
                highest_price = "{:,}".format(int(highest_price_copy / 10))
                highest_time = highest_time_copy
                lowest_time = lowest_time_copy
            except:
                lowest_price,highest_price,highest_time,lowest_time="","","",""

        try:
            growth_month = round(((now_mean - days_history_month_mean) / days_history_month_mean) * 100, 2)
            if abs(growth_month)<0.01:
                growth_month="0.00"
            growth_month_situ = situ(now_mean, days_history_month_mean)
        except:
            print('ERROR 9')
            growth_month = "-"
            growth_month_situ = emoji.emojize(':radio_button:') + " "

        try:
            growth_week = round(((now_mean - days_history_week_mean) / days_history_week_mean) * 100, 2)
            if abs(growth_week)<0.01:
                growth_week="0.00"
            growth_week_situ = situ(now_mean, days_history_week_mean)
        except:
            print('ERROR 10')
            growth_week = "-"
            growth_week_situ = emoji.emojize(':radio_button:') + " "

        try:
            growth_24 = round(((now_mean - days_history_24mean) / days_history_24mean) * 100, 2)
            if abs(growth_24) < 0.01:
                growth_24 = '0.00'
            growth_24_situ = situ(now_mean, days_history_24mean)
        except:
            print('ERROR 11')
            growth_24 = "-"
            growth_24_situ = emoji.emojize(':radio_button:') + " "
        try:
            growth_1 = round(((now_mean - hours_history_1mean) / hours_history_1mean) * 100, 2)
            if abs(growth_1) < 0.01:
                growth_1 = '0.00'
            growth_1_situ = situ(now_mean, hours_history_1mean)
        except:
            print('ERROR 12')
            growth_1 = "-"
            growth_1_situ = emoji.emojize(':radio_button:') + " "

        # try:
        #     # using just valid price to report as mean in message
        #     now_mean = int(prices.iloc[:-6].mean())
        # except:
        #     print('ERROR 13')
        #     now_mean = int(prices2.mean())

        try:
            bubble = now_mean - int(dollar_based_price_copy)
            bubble_situ = situ(now_mean, int(dollar_based_price_copy))
            bubble = "{:,}".format(int(bubble))
        except Exception as e:
            print('ERROR 14')
            bubble = ""
            bubble_situ = emoji.emojize(':radio_button:') + " "
            print('bubble ;', e)

        try:
            coin_value =0.9*8.133*dollar_price_copy*ounce_price_copy/ 31.1034768
            bubble_coin = int(coin_price_copy) - coin_value
            bubble_coin_copy =int(bubble_coin)
            try:
                bubble_coin_situ = situ(bubble_coin, int(days_history_24['bubble_coin']))
                bubble_coin_dif = np.round(((bubble_coin - days_history_24['bubble_coin']) / days_history_24['bubble_coin']) * 100,
                                           2)
                if abs(bubble_coin_dif) < 0.01:
                    bubble_coin_dif = "0.00"
            except Exception as e:
                print(e)
                bubble_coin_situ=emoji.emojize(':radio_button:') + " "
                bubble_coin_dif=""

            bubble_coin = "{:,}".format(int(bubble_coin))
        except Exception as e :
            print('ERROR 15')
            bubble_coin = "-"
            bubble_coin_situ = emoji.emojize(':radio_button:') + " "
            bubble_coin_dif=""
            print('bubble_coin ;', e)
        try:
            now_mean = "{:,}".format(int(now_mean))
        except:
            print('ERROR 16')

        prices = pd.DataFrame(prices).T
        try:
            prices['bubble_coin']= bubble_coin_copy
        except:
            print('ERROR 17')
            prices['bubble_coin']= ""
        prices.insert(0, 'time', Times_min)
        prices.insert(0, 'hour', Times_hour)
        prices.index = pd.Series(df_jalalidate).astype(str)
        jalali_date = str(df_jalalidate)

        month_mean = month_mean_copy.copy()
        week_mean = week_mean_copy.copy()
        days_history_24 = days_history_24_copy.copy()
        hours_history_1 = hours_history_1_copy.copy()


        if n == 1:
            first_time=True
            Alarm_send = False
            Alarm_send_ounce = False
            positive24_ounce_price = False
            positive24 = False
            positive1 = False

        try:
            if first_time == True or day_change == True:
                if float(growth_24) >= 0:
                    pos_last_growth_24 = float(growth_24)
                    neg_last_growth_24 = 0
                if float(growth_1) >= 0:
                    pos_last_growth_1 = float(growth_1)
                    neg_last_growth_1 = 0
                if float(growth_24) < 0:
                    neg_last_growth_24 = float(growth_24)
                    pos_last_growth_24 = 0
                if float(growth_1) < 0:
                    neg_last_growth_1 = float(growth_1)
                    pos_last_growth_1 = 0
                try:
                    if float(ounce_dif) >= 0:
                        pos_last_growth_24_ounce_price = float(ounce_dif)
                        neg_last_growth_24_ounce_price = 0
                    if float(ounce_dif) < 0:
                        neg_last_growth_24_ounce_price = float(ounce_dif)
                        pos_last_growth_24_ounce_price = 0
                except:
                    pos_last_growth_24_ounce_price=neg_last_growth_24_ounce_price=0
            if hour_change==True:
                if float(growth_1) < 0:
                    neg_last_growth_1 = float(growth_1)
                    pos_last_growth_1 = 0
                if float(growth_1) > 0:
                    pos_last_growth_1 = float(growth_1)
                    neg_last_growth_1 = 0

            first_time = False
            try:
                try:
                    if float(ounce_dif) - pos_last_growth_24_ounce_price>alarm:
                        if float(ounce_dif) >= 0:
                            positive24_ounce_price = True
                        neg_last_growth_24_ounce_price = 0
                        #دوشنبه یه دفعه باز میشه
                        pos_last_growth_24_ounce_price = ounce_dif

                        Alarm_send_ounce = True
                except:
                    print('1955')

                if (float(growth_24) - pos_last_growth_24 > alarm) or (float(growth_1) - pos_last_growth_1 > alarm):
                    if float(growth_24) >= 0:
                        positive24 = True
                        neg_last_growth_24 = 0
                    if float(growth_1) >= 0:
                        positive1 = True
                        neg_last_growth_1 = 0
                    if growth_24>pos_last_growth_24:
                        pos_last_growth_24 = growth_24
                    if growth_1 > 0:
                        pos_last_growth_1 = growth_1
                    Alarm_send = True


                try:
                    if float(ounce_dif) -neg_last_growth_24_ounce_price < -alarm:
                        if float(ounce_dif) >= 0:
                            positive24_ounce_price = True
                        pos_last_growth_24_ounce_price = 0
                        #دوشنبه یه دفعه باز میشه
                        neg_last_growth_24_ounce_price = ounce_dif

                        Alarm_send_ounce = True
                except:
                    print('1985')

                if (float(growth_24) - neg_last_growth_24 < -alarm) or (float(growth_1) - neg_last_growth_1 < -alarm):
                    if float(growth_24) >= 0:
                        positive24 = True
                    else:
                        pos_last_growth_24 = 0
                    if float(growth_1) >= 0:
                        positive1 = True
                    else:
                        pos_last_growth_1 = 0
                    if growth_24<neg_last_growth_24:
                        neg_last_growth_24 = growth_24
                    if growth_1<0:
                        neg_last_growth_1 = growth_1
                    Alarm_send = True
                print('last pos (24) ounce, ', round(pos_last_growth_24_ounce_price, 2))
                print('last pos (24), ', round(pos_last_growth_24,2))
                print('last pos (1), ', round(pos_last_growth_1,2))
                print('last neg (24) ounce, ', round(neg_last_growth_24_ounce_price, 2))
                print('last neg (24), ', round(neg_last_growth_24,2))
                print('last neg (1), ', round(neg_last_growth_1,2))
            except Exception as e:
                print("Email Alarm erorr is : ", e)
                traceback.print_exc()
            day_change = False
            hour_change = False

        except Exception as e:
            print('Sending Alarm failed because', e.message)
            traceback.print_exc()

        try:

            if Alarm_send == True:
                if Alarm_send_num>30:
                    Alarm_send = False
                    positive24 = False
                    positive1 = False
                    Alarm_send_num=0
                else:
                    Alarm_send_num=+1
                    send_telegram2(Times_min, df_jalalidate,positive24,positive1,now_mean,highest_price,lowest_price,highest_time,lowest_time,growth_24,growth_1, test)
                    Alarm_send = False
                    positive24 = False
                    positive1 = False
                    Alarm_send_num=0


        except Exception as e:
            print('Sending gold Alarm failed because', e.message)
            traceback.print_exc()

        try:

            if Alarm_send_ounce == True:
                if Alarm_send_ounce_num>30:
                    Alarm_send_ounce = False
                    positive24_ounce_price = False
                    Alarm_send_ounce_num = 0
                else:
                    Alarm_send_ounce_num=+1
                    send_telegram3(positive24_ounce_price,ounce_price,ounce_dif, test)
                    Alarm_send_ounce = False
                    positive24_ounce_price = False
                    Alarm_send_ounce_num=0
                # Email(Times_min, df_jalalidate,positive24, positive1, now_mean, pos_last_growth_24, neg_last_growth_24,growth_24,growth_1)
        except Exception as e:
            print('Sending ounce Alarm failed because', e.message)
            traceback.print_exc()

        try:
            message = message_tel(Times_min, jalali_date, now_mean, rank_name_list, situ_list, rank_dif_list, rank_list,
                                  growth_month, growth_month_situ, growth_week, growth_week_situ, growth_24, growth_24_situ,
                                  growth_1, growth_1_situ,
                                  estjt_price, estjt_situ, estjt_dif, coin_price, coin_situ, coin_dif, ounce_price,
                                  ounce_situ, ounce_dif, dollar_price, dollar_situ, dollar_dif, dollar_based_price,
                                  dollar_based_situ, dollar_based_dif, bubble_situ, bubble,bubble_coin_situ,
                                  bubble_coin,bubble_coin_dif,dublicate_estjt,dublicate_ounce,dublicate_coin,
                                  highest_price,lowest_price,highest_time,lowest_time)
        # sending data to telegram
            try:
                send_telegram(message, test)
            except Exception as e:
                print("telegram send", e)
            dublicate_estjt = dublicate_ounce = dublicate_coin=False
        except Exception as e:
            print('ERROR 18',e)
        # chose last dataline to add to database
        new_line = tuple(list(prices.itertuples())[-1])
        try:
            sqlData(new_line)
        except Exception as e:
            print('ERROR 19', e)
        # processing on data get a message for sharing


        n += 1

        if int(time_next) > 0:
            print(f'sleep for {time_next} sec')
            time.sleep(time_next)

    except Exception as e:
        n += 1
        print('all problem is, ', e)
        traceback.print_exc()
        print('time_next', time_next)
        if int(time_next) > 0:
            print(f'one losed, sleep for {time_next} sec')
            time.sleep(time_next)








