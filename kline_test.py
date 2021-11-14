import os
from time import time, sleep
#from binance.client import Client
import pandas as pd
import sqlalchemy
from binance.enums import *
from binance.streams import ThreadedWebsocketManager

#create engine for data retrieval
engine = sqlalchemy.create_engine('sqlite:///binance.db')

#init dict where we'll receive msg data
coin_price = {'error':False}

#foo for callback to fill the dict with chosen parameters
def coin_trade_history(msg):
    ''' define how to process incoming WebSocket messages '''
    if msg['e'] != 'error':
        #print(msg['E'])
        coin_price['sym'] = msg['s']
        coin_price['time'] = msg['E']
        
        coin_price['kline_start'] = msg['k']['t']
        coin_price['kline_close'] = msg['k']['T']
        coin_price['interv'] = msg['k']['i']
        coin_price['1st_tr_id'] = msg['k']['f']
        coin_price['last_tr_id'] = msg['k']['L']
        coin_price['open'] = msg['k']['o']
        coin_price['close'] = msg['k']['c']
        coin_price['high'] = msg['k']['h']
        coin_price['low'] = msg['k']['l']
        coin_price['base_ass_vol'] = msg['k']['v']
        coin_price['num_trades'] = msg['k']['n']
        coin_price['kline_closed'] = msg['k']['x']
        coin_price['quote_ass_vol'] = msg['k']['q']
        coin_price['taker_buy_base_ass_vol'] = msg['k']['V']
        coin_price['taker_quote_base_ass_vol'] = msg['k']['Q']
        coin_price['error'] = False
        
    else:
        coin_price['error'] = True
        print(ooops)

# init and start the WebSocket
bsm = ThreadedWebsocketManager()
bsm.start()

# subscribe to a stream
#bsm.start_symbol_ticker_socket(callback=btc_trade_history, symbol='BTCUSDT')
bsm.start_kline_socket(callback=coin_trade_history, symbol='BTCUSDT', interval=KLINE_INTERVAL_5MINUTE)
bsm.start_kline_socket(callback=coin_trade_history, symbol='ETHUSDT', interval=KLINE_INTERVAL_5MINUTE)

#init current start timestamp
start_stamp = coin_price['time']

#function converst received dict to df & executes

def createframe(coin_price):
    df = pd.DataFrame([coin_price])
    
    df = df.loc[:, ['sym', 'time', 'kline_start', 'kline_close', 'interv', '1st_tr_id', 'last_tr_id', 'open', 'close', 'high', 'low', 'base_ass_vol','num_trades', 'kline_closed', 'quote_ass_vol', 'taker_buy_base_ass_vol', 'taker_quote_base_ass_vol']] 
    
    df.columns = ['Coin', 'Time', 'Kline start time', 'Kline close time', 'Interval', 'First trade ID', 'First trade ID', 'Open', 'Close', 'High', 'Low', 'Base asset volume', 'Number of trades', 'Is this kline closed?', 'Quote asset volume', 'Taker buy base asset volume', 'Taker buy quote asset volume']
    df['Time'] = pd.to_datetime(df['Time'], unit='ms')
    df['Kline start time'] = pd.to_datetime(df['Kline start time'], unit='ms')
    df['Kline close time'] = pd.to_datetime(df['Kline close time'], unit='ms')
    df['Open'] = df['Open'].astype(float)
    df['Close'] = df['Close'].astype(float)
    df['High'] = df['High'].astype(float)
    df['Low'] = df['Low'].astype(float)
    df['Base asset volume'] = df['Base asset volume'].astype(float)
    return df
createframe(coin_price)

#function to add required candle to SQL DB
def frame_add():
    frame = createframe(coin_price)
    frame.to_sql('binance', engine, if_exists='append', index = False)
    print(frame)

#function to catch msg every n minutes/hours

def coin_catch_msg(coin_price, start_stamp):
    ''' define how to process incoming WebSocket messages '''
    
    
    while coin_price != None: #checks if dict exists for this stamp
        #print(x)
        next_stamp = coin_price['time'] #cashes stamp in next msg
        if next_stamp >= (start_stamp+10000): #1000 = 1s
            print(start_stamp, "it's time")
            print(coin_price)
            frame_add()
            start_stamp = next_stamp
        else: 
            sleep(1)
            #print('sleeping')

#run function
coin_catch_msg(coin_price, start_stamp)