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
import asyncio
from datetime import datetime
from datetime import date
import time
import psycopg2
import smtplib, ssl
import jalali_pandas
import emoji
from telegram.error import TimedOut
import warnings
warnings.filterwarnings('ignore')
import ast
from telegram.ext import Application
from concurrent.futures import ThreadPoolExecutor, TimeoutError
day_change=False

# if need to send message to test chanel vvv
test = True
n = 0
database_tether = 'tether_price_data.db'
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

from telegram import __version__ as TG_VER

import aiohttp

import asyncio
from telegram.ext import Application
from aiohttp_socks import ProxyConnector

async def send_file(message, TOKEN, CHAT_ID, proxy_url="socks5://127.0.0.1:1080"):
    if proxy_url:
        connector = ProxyConnector.from_url(proxy_url)
        app = Application.builder().token(TOKEN).connection_kwargs({"connector": connector}).build()
    else:
        app = Application.builder().token(TOKEN).build()

    async with app:
        await app.bot.send_message(chat_id=CHAT_ID, text=message)
        print("File sent successfully!")

def send_telegram(message, test, proxy_url="socks5://127.0.0.1:1080"):
    config = read_config('tokens.txt')  # your own config function

    if test:
        TOKEN = config['TOKEN_TEST']
        CHAT_ID = config['CHAT_ID_TEST']
    else:
        TOKEN = config['TOKEN_TETHER']
        CHAT_ID = config['CHAT_ID_TETHER']

    try:
        asyncio.run(send_file(message, TOKEN, CHAT_ID, proxy_url))
    except Exception as e:
        print(f"Proxy is off or error occurred: {e}")




def Email(Times_min, df_jalalidate,positive24,positive1,now_mean,pos_last_growth_24,neg_last_growth_24):
    config = read_config('tokens.txt')
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    sender_email = config['SENDER_EMAIL']
    try:
        email_list = config['RECEIVER_EMAIL']
        email_list = ast.literal_eval(email_list)
        receiver_email = tuple(email_list)
    except Exception as e:
        print('ee, ',e)

    if pos_last_growth_24>2.5 or neg_last_growth_24< -2.5:
        email_list=config['RECEIVER_EMAIL2']
        email_list = ast.literal_eval(email_list)
        receiver_email = tuple(email_list)
    password = config['PASSWORD_EMAIL']
    prohibited=emoji.emojize(":warning:")
    pos_neg_sign24=""
    pos_neg_sign1 = ""
    if positive24==True:
        pos_neg_sign24='+'
    if positive24==False:
        pos_neg_sign24 = ''
    if positive1==True:
        pos_neg_sign1='+'
    if positive1==False:
        pos_neg_sign1 = ''
    message = f"""\
    Alarm: دلار
    \n
    درصد تغییرات 24 ساعته دلار
               {prohibited} {pos_neg_sign24}{str(growth_24)} % \n
    درصد تغییرات 1 ساعته دلار
               {prohibited} {pos_neg_sign1}{str(growth_1)} % \n
    ------ میانگین قیمت دلار  ------
                  {now_mean}\n
             ساعت: {str(Times_min)}
          تاریخ:{(df_jalalidate[-1])}"""
    message = message.encode('utf-8')
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context, ) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message)


def time_function(n, time_start, sleep ):
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
                highest_price,lowest_price,highest_time,lowest_time):
    clock = emoji.emojize(':alarm_clock:')
    calendar = emoji.emojize(":spiral_calendar:")
    coin = emoji.emojize("")
    down = emoji.emojize(":down_arrow:")
    up   =   emoji.emojize(":up_arrow:")
    right = emoji.emojize(":right_arrow:")
    left = emoji.emojize(":left_arrow:")
    dollar = emoji.emojize(":heavy_dollar_sign:")
    reminder = emoji.emojize(":pushpin:")
    white = emoji.emojize(":white_small_square:")
    black = emoji.emojize(":black_small_square:")
    dollar=""

    if len(rank_list)>0:
        num0=f'{coin} {rank_name_list[0]} :\n{rank_list[0]}              {situ_list[0]} {rank_dif_list[0]} %\n\n'
    else:
        num0=""
    if len(rank_list)>1:
        num1=f'{coin} {rank_name_list[1]} :\n{rank_list[1]}              {situ_list[1]} {rank_dif_list[1]} %\n\n'
    else:
        num1=""
    if len(rank_list)>2:
        num2=f'{coin} {rank_name_list[2]} :\n{rank_list[2]}              {situ_list[2]} {rank_dif_list[2]} %\n\n'
    else:
        num2=""
    if len(rank_list)>3:
        num3=f'{coin} {rank_name_list[3]} :\n{rank_list[3]}              {situ_list[3]} {rank_dif_list[3]} %\n\n'
    else:
        num3=""
    if len(rank_list)>4:
        num4=f'{coin} {rank_name_list[4]} :\n{rank_list[4]}              {situ_list[4]} {rank_dif_list[4]} %\n\n'
    else:
        num4=""
    if len(rank_list)>5:
        num5=f'{coin} {rank_name_list[5]} :\n{rank_list[5]}              {situ_list[5]} {rank_dif_list[5]} %\n\n'
    else:
        num5=""
    if len(rank_list)>6:
        num6=f'{coin} {rank_name_list[6]} :\n{rank_list[6]}              {situ_list[6]} {rank_dif_list[6]} %\n\n'
    else:
        num6=""
    if len(rank_list)>7:
        num7=f'{coin} {rank_name_list[7]} :\n{rank_list[7]}              {situ_list[7]} {rank_dif_list[7]} %\n\n'
    else:
        num7=""
    if len(rank_list)>8:
        num8=f'{coin} {rank_name_list[8]} :\n{rank_list[8]}              {situ_list[8]} {rank_dif_list[8]} %\n\n'
    else:
        num8=""
    if len(rank_list)>9:
        num9=f'{coin} {rank_name_list[9]} :\n{rank_list[9]}              {situ_list[9]} {rank_dif_list[9]} %\n'
    else:
        num9=""

    # making message based on data
    tether_prices = (f'{dollar} میانگین نرخ دلار:  {now_mean} تومان {growth_24_situ}\n\n'
                     f'{black} حداکثر و حداقل قیمت امروز:\n\n'
                     f'              {up} {highest_price}          AT: {highest_time}\n\n'
                     f'              {down} {lowest_price}          AT: {lowest_time}\n'
                     f'---------------  \n'
                     f'{white} تغییر نسبت به ماه گذشته:\n'
                     f'  {growth_month_situ} {growth_month} % \n\n'
                     f'{white} تغییر نسبت به هفته گذشته:\n'
                     f'  {growth_week_situ} {growth_week} % \n\n'
                     f'{white} تغییر نسبت به روز گذشته:\n'
                     f'  {growth_24_situ} {growth_24} % \n\n'
                     f'{white} تغییر در یک ساعت گذشته:\n'
                     f'  {growth_1_situ} {growth_1} % \n\n'
                     f'---------------  \n'
                     f'{reminder}قیمت های آنی و تغییر 1 ساعته {down}\n'
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
                     f'_______________________  \n'
                     f'                     {calendar}  {(jalali_date)}\n\n'
                     f'                           {clock}  {Times_min}\n'
                     f'\n'
                     f'@Tether_Iran1'
                     )

    tether_prices = tether_prices.replace("]", "")
    tether_prices = tether_prices.replace("[", "")
    message = tether_prices.replace("'", "")

    return message


def sqlData (new_line):
    # Connect to PostgreSQL
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
    cursor.execute('''CREATE TABLE IF NOT EXISTS tether_data
                         (id TEXT,
                          hour text,
                          Time Text,
                          nobitex INTEGER,
                          wallex INTEGER,
                          tabdeal INTEGER,
                          ramzinex INTEGER,
                          exir INTEGER,
                          tetherland INTEGER,
                          ok_ex INTEGER,
                          aban INTEGER,
                          ap INTEGER,
                          bitpin INTEGER)
                          ''')
    insert_sql = '''
            INSERT INTO tether_data VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
    cursor.execute(insert_sql, new_line)
    conn.commit()
    cursor.close()
    conn.close()


    con=sqlite3.connect(database_tether)
    cursor=con.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS tether_data
                         (id TEXT,
                          hour text,
                          Time Text,
                          nobitex INTEGER,
                          wallex INTEGER,
                          tabdeal INTEGER,
                          ramzinex INTEGER,
                          exir INTEGER,
                          tetherland INTEGER,
                          ok_ex INTEGER
                          aban INTEGER,
                          ap INTEGER,
                          bitpin INTEGER)
                          ''')
    cursor.execute("INSERT INTO tether_data VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",new_line)
    con.commit()
    con.close()


def history(n):
    conn = psycopg2.connect(
        dbname='mydb',
        user='myuser',
        password='377843',
        host='localhost',
        port='5432'
    )
    cur = conn.cursor()
    cur.execute("SELECT * FROM tether_data ORDER BY id DESC LIMIT 50000")
    rows = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]

    history = pd.DataFrame(rows, columns=column_names)
    history = history.sort_values(by=history.columns[0], ascending=True)

    days_history=history.groupby('id')

    if n==1:
        try:
            formonthhistory=history.copy()
            formonthhistory.replace('', np.nan, inplace=True)
            formonthhistory = formonthhistory.fillna(method='bfill')
            # formonthhistory = formonthhistory.dropna()
            formonthhistory.index=formonthhistory['id']
            mon30=formonthhistory.id.unique()[-31]
            mon0=formonthhistory.id.unique()[-31]
            month=formonthhistory.loc[mon30:mon0]
            month_mean = month.iloc[:, 3:].astype(float).mean(axis=0)
            week7=formonthhistory.id.unique()[-8]
            week0 = formonthhistory.id.unique()[-8]
            week = formonthhistory.loc[week7:week0]
            week_mean=week.iloc[:, 3:].astype(float).mean(axis=0)
        except Exception as e:
            print('monthly or weekly erorr, ',e)

    group_day1=history.id.unique()[-2]
    days_history1 = days_history.get_group(group_day1)
    days_history1 = days_history1.replace('', np.nan)
    days_history1.interpolate(method='linear', inplace=True)
    days_history1.interpolate(method='ffill', inplace=True)
    days_history1.interpolate(method='bfill', inplace=True)

    group_day2=history.id.unique()[-1]
    days_history2 = days_history.get_group(group_day2)
    days_history2 = days_history2.replace('', np.nan)
    days_history2.interpolate(method='linear', inplace=True)
    days_history2.interpolate(method='ffill', inplace=True)
    days_history2.interpolate(method='bfill', inplace=True)
    try:
        today_max, today_min = days_history2.iloc[:, 3:].mean(axis=1).max(), days_history2.iloc[:, 3:].mean(
            axis=1).min()
        highest_time=days_history2.loc[days_history2.iloc[:, 3:].mean(axis=1).idxmax(),'Time']
        lowest_time=days_history2.loc[days_history2.iloc[:, 3:].mean(axis=1).idxmin(),'Time']
    except:
        today_max, today_min,highest_time ,lowest_time= "", "","", ""
    hours_history = days_history2.groupby('hour')
    last_two_hours = list(hours_history.groups.keys())[-2:]
    hours_history = hours_history.get_group(last_two_hours[-1])
    # select the group corresponding to the last hour

    try:
        days_history=days_history1.iloc[:,3:].astype(int).mean(axis=0)
    except:
        i_c = 3
        for i in hours_history.columns:
            days_history1.iloc[:, i_c] = pd.to_numeric(days_history1.iloc[:, i_c], errors="coerce")
            i_c += 1
            if i_c == 13:
                days_history1 = days_history1.fillna(method='ffill')
                try:
                    days_history = days_history1.iloc[:, 3:].astype(int).mean(axis=0)
                except:
                    days_history1=days_history1.T
                    mean = days_history1.iloc[3:, :].mean()
                    days_history1=days_history1.fillna(mean)
                    days_history1=days_history1.T
                    days_history = days_history1.iloc[:, 3:].astype(int).mean(axis=0)
                break
    try:
        hours_history=hours_history.iloc[:,3:].astype(int).mean(axis=0)
    except:
        i_c = 3
        for i in hours_history.columns:
            hours_history.iloc[:, i_c] = pd.to_numeric(hours_history.iloc[:, i_c], errors="coerce")
            i_c += 1
            if i_c == 13:
                hours_history = hours_history.fillna(method='ffill')
                try:
                    hours_history = hours_history.iloc[:, 3:].astype(int).mean(axis=0)
                except:
                    hours_history=hours_history.T
                    mean = hours_history.iloc[3:, :].mean()
                    hours_history=hours_history.fillna(mean)
                    hours_history=hours_history.T
                    hours_history = hours_history.iloc[:, 3:].astype(int).mean(axis=0)
                break

    conn.commit()
    conn.close()
    if n==1:
        try:
            return days_history,hours_history,month_mean, week_mean,today_max,today_min,highest_time,lowest_time
        except:
            month_mean="-"
            week_mean="-"
            return days_history, hours_history, month_mean, week_mean,today_max,today_min,highest_time,lowest_time
    else:
        return days_history, hours_history

def history2():
    conn = psycopg2.connect(
        dbname='mydb',
        user='myuser',
        password='377843',
        host='localhost',
        port='5432'
    )
    cur = conn.cursor()
    cur.execute("SELECT * FROM tether_data ORDER BY id DESC LIMIT 50000")
    rows = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]

    history = pd.DataFrame(rows, columns=column_names)
    history = history.sort_values(by=history.columns[0], ascending=True)
    days_history=history.groupby('id')

    try:
        formonthhistory = history.copy()
        formonthhistory.replace('', np.nan, inplace=True)
        formonthhistory = formonthhistory.fillna(method='bfill')
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

    # separate today's data to get last hour data from it (without last column that is global ounce gold)
    group_day=history.id.unique()[-1]
    days_history = days_history.get_group(group_day)
    days_history = days_history.replace('', np.nan)
    days_history.interpolate(method='linear', inplace=True)
    days_history.interpolate(method='ffill', inplace=True)
    days_history.interpolate(method='bfill', inplace=True)
    hours_history = days_history.groupby('hour')
    last_two_hours = list(hours_history.groups.keys())[-2:]
    hours_history = hours_history.get_group(last_two_hours[-1])
    # select the group corresponding to the last hour
    try:
        days_history = days_history.iloc[:, 3:].astype(int).mean(axis=0)
    except:
        i_c = 3
        for i in hours_history.columns:
            days_history.iloc[:, i_c] = pd.to_numeric(days_history.iloc[:, i_c], errors="coerce")
            i_c += 1
            if i_c == 13:
                days_history = days_history.fillna(method='ffill')
                try:
                    days_history = days_history.iloc[:, 3:].astype(int).mean(axis=0)
                except:
                    days_history = days_history.T
                    mean = days_history.iloc[3:, :].mean()
                    days_history = days_history.fillna(mean)
                    days_history = days_history.T
                    days_history = days_history.iloc[:, 3:].astype(int).mean(axis=0)
                break
    try:
        hours_history = hours_history.iloc[:, 3:].astype(int).mean(axis=0)
    except:
        i_c = 3
        for i in hours_history.columns:
            hours_history.iloc[:, i_c] = pd.to_numeric(hours_history.iloc[:, i_c], errors="coerce")
            i_c += 1
            if i_c == 13:
                hours_history = hours_history.fillna(method='ffill')
                try:
                    hours_history = hours_history.iloc[:, 3:].astype(int).mean(axis=0)
                except:
                    hours_history = hours_history.T
                    mean = hours_history.iloc[3:, :].mean()
                    hours_history = hours_history.fillna(mean)
                    hours_history = hours_history.T
                    hours_history = hours_history.iloc[:, 3:].astype(int).mean(axis=0)
                break
    conn.commit()
    conn.close()
    try:
        return days_history,hours_history,month_mean, week_mean
    except:
        month_mean="-"
        week_mean="-"
        return days_history, hours_history, month_mean, week_mean



def nobitex(url_nobitex, prices, names):
    try:
        names.append('nobitex')
        response = requests.get(url_nobitex,verify=False, timeout=15)  # Replace with your URL
        data = response.json()
        nobitex_price = data['stats']['usdt-rls']['latest']
        if len(str(nobitex_price)) < 6:
            nobitex_price = int((int(nobitex_price)) * 10)
        prices.append(int(nobitex_price))
    except:
        prices.append("")
        pass
    return prices, names
        

def wallex(url_wallex, prices, names):
    try:
        names.append('wallex')
        response = requests.get(url_wallex,verify=False, timeout=15)
        data = response.json()
        wallex_price = int(float(data['result']['latestTrades'][0]['price']))
        wallex_price = int((int(wallex_price)) * 10)
        prices.append(int(wallex_price))
    except:
        prices.append("")
        pass
    return prices, names


def tabdeal(url_tabdeal, prices, names):
    try:
        names.append('tabdeal')
        response = requests.get(url_tabdeal,verify=False, timeout=15)
        data = response.json()
        x = next((item for item in data if item['name'] == 'TetherUS'), None)
        tabdeal_price = int(x['markets'][0]['price'])
        tabdeal_price = int((int(tabdeal_price)) * 10)
        prices.append(int(tabdeal_price))
    except:
        prices.append("")
        pass
    return prices, names


def ramzinex(url_ramzinex, prices, names):
    try:
        names.append('ramzinex')
        response = requests.get(url_ramzinex,verify=False, timeout=15)
        data = json.loads(response.text)
        ramzinex_price = data['close']
        if len(str(ramzinex_price)) < 6:
            ramzinex_price = int((int(ramzinex_price)) * 10)
        prices.append(int(ramzinex_price))
    except:
        prices.append("")
        pass
    return prices, names


def exir(url_exir, prices, names):
    try:
        names.append('exir')
        response = requests.get(url_exir,verify=False, timeout=15)
        data = response.json()
        exir_price = data['close']
        exir_price = int((int(exir_price)) * 10)
        prices.append(int(exir_price))
    except:
        prices.append("")
        pass
    return prices, names


def tetherland(url_tetherland, prices, names):
    try:
        names.append('tetherland')
        headers = {"Accept": "application/json"}
        response = requests.get(url_tetherland, headers=headers,verify=False, timeout=15)
        data = response.json()
        tetherland_price = data['data']['currencies']['USDT']['price']
        tetherland_price = int((int(tetherland_price)) * 10)
        prices.append(int(tetherland_price))
    except:
        prices.append("")
        pass
    return prices, names

def ok_ex(url_ok_ex, prices, names):
    try:
        names.append('ok_ex')
        response = requests.get(url_ok_ex,verify=False, timeout=15)
        data = response.json()
        ok_ex_price = data['tickers'][0]['last']
        ok_ex_price = (re.sub(r"\D", "", ok_ex_price))
        if len(str(ok_ex_price)) < 6:
            ok_ex_price = int((int(ok_ex_price)) * 10)
        prices.append(int(ok_ex_price))
    except:
        prices.append("")
        pass
    return prices, names


def aban(url_aban, prices, names):
    try:
        names.append('aban')
        response = requests.get(url_aban,verify=False, timeout=15)
        soup = BeautifulSoup(response.content, 'html.parser')
        value = soup.find('div', {'class': 'Body_row__ta6o9'})
        aban_price = value.find("div").text
        aban_price = aban_price.replace(',', '')
        aban_price = int(aban_price)
        aban_price = int((int(aban_price)) * 10)
        prices.append(int(aban_price))
    except:
        prices.append("")
        pass
    return prices, names



async def ap(url_ap, prices, names):
    try:
        names.append('ap')
        async with websockets.connect(url_ap) as websocket:
            message = {
                "channel": "markets",
                "model": "all",
                "request": "SUBSCRIBE"
            }
            await asyncio.wait_for(websocket.send(json.dumps(message)), timeout=10)

            # Adding timeout to receiving data
            json_data = await asyncio.wait_for(websocket.recv(), timeout=10)
            data = json.loads(json_data)
            ap_price = next((market['ask_price'] for market in data['message']['markets'] if market['market'] == 'usdtirr'), None)



            prices.append(int(ap_price))
    except:
        prices.append("")
        pass
    return prices, names



def bitpin(url_bitpin, prices, names):
    try:
        names.append('bitpin')
        response = requests.get(url_bitpin,verify=False, timeout=15)
        data = response.json()
        eigen_usdt_data = next((item for item in data if item['symbol'] == 'USDT_IRT'), None)
        bitpin_price = eigen_usdt_data['price']
        bitpin_price = int((int(bitpin_price)) * 10)
        prices.append(int(bitpin_price))
    except:
        prices.append("")
        pass

    return prices, names

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
        # source data address
        url_nobitex = "https://api.nobitex.ir/market/stats?srcCurrency=usdt&dstCurrency=rls"
        url_wallex = "https://api.wallex.ir/v1/trades?symbol=USDTTMN"
        url_tabdeal = "https://api-web.tabdeal.org/r/plots/currency_prices/"
        url_ramzinex = "https://publicapi.ramzinex.com/exchange/api/v1.0/exchange/chart/statistics-24/11"
        url_exir = "https://api.exir.io/v2/ticker?symbol=usdt-irt"
        url_tetherland = "https://api.tetherland.com/currencies"
        url_ok_ex = "https://azapi.ok-ex.io/oapi/v1/market/tickers"
        url_aban = 'https://abantether.com/coin/USDT'
        url_ap = "wss://cryptian.com/ws"
        url_bitpin = "https://api.bitpin.ir/api/v1/mkt/tickers/"
        try:
            prices, names = nobitex(url_nobitex, prices, names)
            prices, names = wallex(url_wallex, prices, names)
            prices, names = tabdeal(url_tabdeal, prices, names)
            prices, names = ramzinex(url_ramzinex, prices, names)
            prices, names = exir(url_exir, prices, names)
            prices, names = tetherland(url_tetherland, prices, names)
            prices, names = ok_ex(url_ok_ex, prices, names)
            prices, names = aban(url_aban, prices, names)
            prices, names = asyncio.run(ap(url_ap, prices, names))
            prices, names = bitpin(url_bitpin, prices, names)
            prices = pd.Series(prices)
            prices.index = names
        except Exception as e:
            print('price, names, ',e)
        try:
            prices = prices.replace('', 0)
            prices = prices.astype(float)
            prices = prices.replace(0, "")
        except:
            print('ERROR 2')

        return prices.T
    except:
        prices = pd.DataFrame(prices)
        print('ERROR 3')
        return prices.T



def sec_func(prices, hours_history_1):
    rank_list = []
    rank_dif_list = []
    rank_name_list = []
    situ_list = []


    try:
        try:
            nobitex_situ = situ(prices.nobitex, hours_history_1.nobitex)
            nobitex_dif = np.round(((prices.nobitex - hours_history_1.nobitex) / hours_history_1.nobitex) * 100, 2)
            if abs(nobitex_dif)<0.01:
                nobitex_dif='0.00'
        except:
            nobitex_situ = emoji.emojize(':radio_button:') + " "
            nobitex_dif = ""
        rank_list.append(prices.nobitex)
        rank_name_list.append('نوبیتکس')
        situ_list.append(nobitex_situ)
        rank_dif_list.append(nobitex_dif)
    except:
        pass

    try:
        try:
            wallex_situ = situ(prices.wallex, hours_history_1.wallex)
            wallex_dif = np.round(((prices.wallex - hours_history_1.wallex) / hours_history_1.wallex) * 100, 2)
            if abs( wallex_dif)<0.01:
                 wallex_dif='0.00'
        except:
            wallex_situ = emoji.emojize(':radio_button:') + " "
            wallex_dif = ""
        rank_list.append(prices.wallex)
        rank_name_list.append('والکس')
        situ_list.append(wallex_situ)
        rank_dif_list.append(wallex_dif)
    except:
        pass

    try:
        try:
            tabdeal_situ = situ(prices.tabdeal, hours_history_1.tabdeal)
            tabdeal_dif = np.round(((prices.tabdeal - hours_history_1.tabdeal) / hours_history_1.tabdeal) * 100, 2)
            if abs( tabdeal_dif)<0.01:
                 tabdeal_dif='0.00'
        except:
            tabdeal_situ = emoji.emojize(':radio_button:') + " "
            tabdeal_dif = ""
        rank_list.append(prices.tabdeal)
        rank_name_list.append('تبدیل')
        situ_list.append(tabdeal_situ)
        rank_dif_list.append(tabdeal_dif)
    except:
        pass

    try:
        try:
            ramzinex_situ = situ(prices.ramzinex, hours_history_1.ramzinex)
            ramzinex_dif = np.round(((prices.ramzinex - hours_history_1.ramzinex) / hours_history_1.ramzinex) * 100, 2)
            if abs(ramzinex_dif)<0.01:
                ramzinex_dif='0.00'
        except:
            ramzinex_situ = emoji.emojize(':radio_button:') + " "
            ramzinex_dif = ""
        rank_list.append(prices.ramzinex)
        rank_name_list.append('رزمینکس')
        situ_list.append(ramzinex_situ)
        rank_dif_list.append(ramzinex_dif)
    except:
        pass

    try:
        try:
            exir_situ = situ(prices.exir, hours_history_1.exir)
            exir_dif = np.round(((prices.exir - hours_history_1.exir) / hours_history_1.exir) * 100, 2)
            if abs(exir_dif)<0.01:
                exir_dif='0.00'
        except:
            exir_situ = emoji.emojize(':radio_button:') + " "
            exir_dif = ""
        rank_list.append(prices.exir)
        rank_name_list.append('اکسیر')
        situ_list.append(exir_situ)
        rank_dif_list.append(exir_dif)
    except:
        pass

    try:
        try:
            tetherland_situ = situ(prices.tetherland, hours_history_1.tetherland)
            tetherland_dif = np.round(((prices.tetherland - hours_history_1.tetherland) / hours_history_1.tetherland) * 100, 2)
            if abs(tetherland_dif)<0.01:
                tetherland_dif='0.00'
        except:
            tetherland_situ = emoji.emojize(':radio_button:') + " "
            tetherland_dif = ""
        rank_list.append(prices.tetherland)
        rank_name_list.append('تترلند')
        situ_list.append(tetherland_situ)
        rank_dif_list.append(tetherland_dif)
    except:
        pass

    try:
        try:
            ok_ex_situ = situ(prices.ok_ex, hours_history_1.ok_ex)
            ok_ex_dif = np.round(((prices.ok_ex - hours_history_1.ok_ex) / hours_history_1.ok_ex) * 100, 2)
            if abs(ok_ex_dif)<0.01:
                ok_ex_dif='0.00'
        except:
            ok_ex_situ = emoji.emojize(':radio_button:') + " "
            ok_ex_dif = ""
        rank_list.append(prices.ok_ex)
        rank_name_list.append('اوکی اکسچنج')
        situ_list.append(ok_ex_situ)
        rank_dif_list.append(ok_ex_dif)
    except:
        pass

    try:
        try:
            aban_situ = situ(prices.aban, hours_history_1.aban)
            aban_dif = np.round(((prices.aban - hours_history_1.aban) / hours_history_1.aban) * 100, 2)
            if abs(aban_dif)<0.01:
                aban_dif='0.00'
        except:
            aban_situ = emoji.emojize(':radio_button:') + " "
            aban_dif = ""
        rank_list.append(prices.aban)
        rank_name_list.append('آبان تتر')
        situ_list.append(aban_situ)
        rank_dif_list.append(aban_dif)
    except:
        pass

    try:
        try:
            ap_situ = situ(prices.ap, hours_history_1.ap)
            ap_dif = np.round(((prices.ap - hours_history_1.ap) / hours_history_1.ap) * 100, 2)
            if abs(ap_dif)<0.01:
                ap_dif='0.00'
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
            bitpin_situ = situ(prices.bitpin, hours_history_1.bitpin)
            bitpin_dif = np.round(((prices.bitpin - hours_history_1.bitpin) / hours_history_1.bitpin) * 100, 2)
            if abs(bitpin_dif)<0.01:
                bitpin_dif='0.00'
        except:
            bitpin_situ = emoji.emojize(':radio_button:') + " "
            bitpin_dif = ""
        rank_list.append(prices.bitpin)
        rank_name_list.append('بیت پین')
        situ_list.append(bitpin_situ)
        rank_dif_list.append(bitpin_dif)
    except:
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
        print("ERROR 3.5,",e)
        print(rank_list)
        print(rank_name_list)

    # add comma to data
    pos = 0
    for price in rank_list:
        try:
            price = "{:,}".format(int(price/10))
            rank_list[pos] = price
            pos += 1
        except:
            print('ERROR 4')



    return rank_list, rank_name_list, situ_list, rank_dif_list, prices


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

        with ThreadPoolExecutor() as executor:
            future = executor.submit(getting_data)
            try:
                prices=future.result(timeout=120)
            except TimeoutError:
                print("Timeout occurred")
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
                pos_last_growth_24=0
                pos_last_growth_1 = 0
                neg_last_growth_24 = 0
                neg_last_growth_1 = 0
                try:
                    days_history_24, hours_history_1, month_mean, week_mean,today_max,today_min,highest_time,lowest_time = history(n)
                    Times_hour_last = Times_hour
                    jalali_date_last = day
                except:
                    Times_hour_last = Times_hour
                    jalali_date_last =  day
            if Times_hour != Times_hour_last:
                pos_last_growth_1 = 0
                neg_last_growth_1 = 0
                print ('hour changed')
                print ('day, ', day)
                print('jalali_date_last, ', jalali_date_last)

                # when hours change
                days_history_24, hours_history_1 = "", ""
                print("test")
                try:
                    days_history_24, hours_history_1 = history(n)
                except Exception as e:
                    print(e)
                print('days_history_24', days_history_24)
                print('hours_history_1', hours_history_1)
                print('Times_hour')
                print('Times_hour_last')
                Times_hour_last = Times_hour
                print('Times_hour')
                print('Times_hour_last')

                try:
                    if Times_hour=='12':
                        repo_path = os.getcwd()
                        file_to_add = repo_path + '\\' + 'tether_price_data.db'

                        commit_message = f'Add file to repository at {df_jalalidate[-1]}'
                        os.chdir(repo_path)
                        # subprocess.run(['git', 'add', file_to_add])
                        subprocess.run(['git', 'pull'])
                        subprocess.run(['git', 'add', "--all"])
                        subprocess.run(['git', 'commit', '-m', commit_message])
                        subprocess.run(['git', 'push'])
                except Exception as e:
                    print('github error, ', e)
                # The use of checking the time before checking the day is that, getting the last hour information, when the day doesn't change, is on different based than when it does.
                if jalali_date_last !=  day:  #when days change
                    print('day changed')
                    days_history_24, hours_history_1, month_mean, week_mean = "", "", "", ""
                    days_history_24, hours_history_1, month_mean, week_mean = history2()
                    pos_last_growth_24 = 0
                    pos_last_growth_1 = 0
                    neg_last_growth_24 = 0
                    neg_last_growth_1 = 0
                    day_change = True
                    jalali_date_last =  day

            else:
                print()
        except:
            pass


        try:
            days_history_week_mean = week_mean.mean()
            days_history_month_mean = month_mean.mean()
        except:
            print('ERROR 7')
        try:
            days_history_24mean = days_history_24.mean()
            hours_history_1mean = hours_history_1.mean()
        except:
            print('ERROR 8')

        try:
            rank_list, rank_name_list, situ_list, rank_dif_list, prices = sec_func(prices, hours_history_1)
        except Exception as e:
            print(e)
            print('ERROR Baqerzadeh:)')


        # if n==1:
        #     prices3 = prices.copy()
        #     n2=0
        # try:
        #     price_check=pd.concat([prices,prices3],axis=1).T
        #     price_check=price_check.replace("",np.nan).dropna(axis=1).pct_change().iloc[-1] * 100
        #     price_check=price_check[abs(price_check) >= 1]
        #     for i in price_check.index:
        #         price_drop=i
        #         n2=n+10
        #     if n2 >= n:
        #         prices[f'{price_drop}'] = ""
        # except Exception as e:
        #     print('price_check, ',e)
        # prices3=prices.copy()

        try:
            now_mean_zero = prices.mean()
        except:
            print('ERROR 5')
            try:
                prices2 = prices.copy()
                prices2 = prices2.replace('', np.nan)
                prices2 = prices2.dropna()
                now_mean_zero = prices2.mean()
            except Exception as e:
                prices2 = ""
                print('after ERROR 5', e)
        try:
            prices4=prices.copy()
            for colu in prices4.index:
                try:
                    if abs((prices4[f"{colu}"]-now_mean_zero)/now_mean_zero*100)>1.5:
                        prices4[f"{colu}"]=""
                except:
                    pass
            prices2 = prices4.copy()
            prices2 = prices2.replace('', np.nan)
            prices2 = prices2.dropna()
            now_mean = prices2.mean()
        except:
            now_mean=now_mean_zero
            pass

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
            lowest_price="{:,}".format(int(lowest_price_copy/ 10))
            highest_price="{:,}".format(int(highest_price_copy/ 10))
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
            if abs( growth_month)<0.01:
                 growth_month='0.00'
            growth_month_situ = situ(now_mean, days_history_month_mean)
        except:
            print('ERROR 9')
            growth_month = ""
            growth_month_situ = emoji.emojize(':radio_button:') + " "

        try:
            growth_week = round(((now_mean - days_history_week_mean) / days_history_week_mean) * 100, 2)
            if abs( growth_week)<0.01:
                 growth_week='0.00'
            growth_week_situ = situ(now_mean, days_history_week_mean)
        except:
            print('ERROR 10')
            growth_week = ""
            growth_week_situ = emoji.emojize(':radio_button:') + " "

        try:
            growth_24 = round(((now_mean - days_history_24mean) / days_history_24mean) * 100, 2)
            if abs( growth_24)<0.01:
                 growth_24='0.00'
            growth_24_situ = situ(now_mean, days_history_24mean)
        except:
            print('ERROR 11')
            growth_24 = ""
            growth_24_situ = emoji.emojize(':radio_button:') + " "
        try:
            growth_1 = round(((now_mean - hours_history_1mean) / hours_history_1mean) * 100, 2)
            if abs( growth_1)<0.01:
                 growth_1='0.00'
            growth_1_situ = situ(now_mean, hours_history_1mean)
        except:
            print('ERROR 12')
            growth_1 = ""
            growth_1_situ = emoji.emojize(':radio_button:') + " "

        # try:
        #     # using just valid price to report as mean in message
        #     now_mean = int(prices.mean())
        # except:
        #     print('ERROR 13')
        #     prices2 = prices.replace('', np.nan)
        #     prices2 = prices2.dropna()
        #     now_mean = int(prices2.mean())


        try:
            now_mean = "{:,}".format(int(now_mean/10))
        except:
            print('ERROR 16')

        prices = pd.DataFrame(prices).T

        prices.insert(0, 'time', Times_min)
        prices.insert(0, 'hour', Times_hour)
        prices.index = pd.Series(df_jalalidate).astype(str)
        jalali_date = str(df_jalalidate)

        Email_send = False
        positive24 = False
        positive1 = False
        alarm = 1
        if n == 1:
            first_time=True

        try:
            if first_time == True or day_change == True:
                if growth_24 > 0:
                    pos_last_growth_24 = growth_24
                    pos_last_growth_1 = growth_1
                if growth_24 < 0:
                    neg_last_growth_24 = growth_24
                    neg_last_growth_1 = growth_1
            first_time = False
            try:
                if growth_24 - pos_last_growth_24 >= alarm:
                    if growth_24 >= 0:
                        positive24 = True
                    if growth_1 >= 0:
                        positive1 = True
                    pos_last_growth_24 += alarm
                    pos_last_growth_1 += alarm
                    Email_send = True

                if growth_24 - neg_last_growth_24 < -alarm:
                    neg_last_growth_24 += -alarm
                    neg_last_growth_1 += -alarm
                    Email_send = True
                print('last pos (24), ', round(pos_last_growth_24,2))
                print('last pos (1), ', round(pos_last_growth_1,2))
                print('last neg (24), ', round(neg_last_growth_24,2))
                print('last pos (1), ', round(neg_last_growth_1,2))
            except Exception as e:
                print("Email Alarm erorr is : ", e)
            day_change = False
            if Email_send == True:

                Email(Times_min, df_jalalidate,positive24, positive1, now_mean, pos_last_growth_24, neg_last_growth_24)
        except Exception as e:
            print('Sending Alaram failed because', e.message)

        try:
            message = message_tel(Times_min, jalali_date, now_mean, rank_name_list, situ_list, rank_dif_list, rank_list,
                                  growth_month, growth_month_situ, growth_week, growth_week_situ, growth_24, growth_24_situ,
                                  growth_1, growth_1_situ,highest_price,lowest_price,highest_time,lowest_time)

        # sending data to telegram
            try:
                send_telegram(message, test)
            except Exception as e:
                print("telegram send", e)
        except:
            print('ERROR 18')
        # chose last dataline to add to database
        new_line = tuple(list(prices.itertuples())[-1])
        try:
            sqlData(new_line)
        except:
            print('ERROR 19')
        # processing on data get a message for sharing


        n += 1

        if int(time_next) > 0:
            print(f'sleep for {time_next} sec')
            time.sleep(time_next)

    except Exception as e:
        n += 1
        print('all problem is, ', e)
        print('time_next', time_next)
        if int(time_next) > 0:
            print(f'one losed, sleep for {time_next} sec')
            time.sleep(time_next)








