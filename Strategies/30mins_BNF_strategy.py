from smartapi import SmartConnect
import pyotp
from smartapi import SmartWebSocket
import datetime
import mysql.connector  

from variables import *


def genearteTotp(totp_token):
    totp = pyotp.TOTP(totp_token).now() 
    return totp

def angleSmartApiConnection():
    obj=SmartConnect(api_key=ANGLE_APIKEY,
                #optional
                #access_token = "your access token",
                #refresh_token = "your refresh_token")
    )
    totp = genearteTotp(ANGLE_TOTP_TOKEN)
    data = obj.generateSession(ANGLE_USERNAME,ANGLE_PASSWORD,totp)
    refreshToken= data['data']['refreshToken'] 
    # feedToken=obj.getfeedToken()
    # CLIENT_CODE = ANGLE_USERNAME
    return obj

def get30MinCandleData(angle_smart_obj):
    from_ist_date = datetime.datetime.now() - datetime.timedelta(minutes=120)
    to_ist_date = datetime.datetime.now() + datetime.timedelta(minutes=5) 
    data = {}
    for each_stock_token in BANKNIFTY_FUT_TOKEN:
        historic_parms = {
            "exchange": "NFO",
            "symboltoken": each_stock_token,
            "interval": "THIRTY_MINUTE", 
            "fromdate": from_ist_date.strftime("%Y-%m-%d %H:%M"), #"2022-11-28 09:20", now.strftime("%m/%d/%Y, %H:%M:%S")
            "todate": to_ist_date.strftime("%Y-%m-%d %H:%M") #"2022-11-28 12:14"
            # "fromdate":"2022-11-29 10:10",
            # "todate":"2022-11-29 11:17"
            }
        d = angle_smart_obj.getCandleData(historic_parms)
        data[each_stock_token] = d
        # data[each_stock_token] 
        # data[each_stock_symbol] = fyers.history(request_json) 
        # print("-----------------------------") 
    return data

def preprocessCandlesData(data):
    for eachKey in data:
        if data[eachKey]['message'] == 'SUCCESS':
            candles_data = data[eachKey]['data']
            candles_data = sorted(candles_data, reverse=True ,key=lambda x: x[0])
            last3_candles_dict = {}
            last3_candles_dict[0] =  candles_data[0]
            last3_candles_dict[-1] = candles_data[1]
            last3_candles_dict[-2] = candles_data[2]
    return last3_candles_dict

def isStrongCandle(open,high,low,close):
    total_candle = high-low
    if close>open:
        body = close-open
    else:
        body = open-close
    percent = body/total_candle
    if percent > 0.4:
        isStrongCandle = True 
    else:
        isStrongCandle = False
    return  isStrongCandle

def strategy(last3_candles_dict):
    #bullish
    #condition-1: 
    if (last3_candles_dict[-1][3] < last3_candles_dict[-2][3]) and (last3_candles_dict[-1][4] < last3_candles_dict[-2][4]) and (last3_candles_dict[0][1] > last3_candles_dict[-1][4]) and (last3_candles_dict[0][4] > last3_candles_dict[0][1]) and (last3_candles_dict[-1][1] > last3_candles_dict[-1][4]):
        isStrongCandle_bool = isStrongCandle(last3_candles_dict[-1][1],last3_candles_dict[-1][2],last3_candles_dict[-1][3],last3_candles_dict[-1][4])
        if isStrongCandle_bool:
            entry = last3_candles_dict[0][4]
            sl = last3_candles_dict[0][3]
            side = 1
            return entry, sl, side
    #bearish
    if (last3_candles_dict[-1][2] > last3_candles_dict[-2][2]) and (last3_candles_dict[-1][4] > last3_candles_dict[-2][4]) and (last3_candles_dict[0][1] < last3_candles_dict[-1][4]) and (last3_candles_dict[0][4] < last3_candles_dict[0][1]) and (last3_candles_dict[-1][1] < last3_candles_dict[-1][4]):
        isStrongCandle_bool = isStrongCandle(last3_candles_dict[-1][1],last3_candles_dict[-1][2],last3_candles_dict[-1][3],last3_candles_dict[-1][4])
        if isStrongCandle_bool:
            entry = last3_candles_dict[0][4]
            sl = last3_candles_dict[0][2]
            side = -1
            return entry, sl, side
    return 0,0,0
    
def executeTrade(angle_smart_obj, entry, sl, side):
    if side>0:
        transactiontype = "BUY"
        squareoff = (entry - sl)*2 
        squareoff = int(squareoff)
    else:
        transactiontype = "SELL"
        squareoff = (sl-entry)*2 
        squareoff = int(squareoff)
    entry = int(entry)
    sl = int(sl)
    
    orderparams = {
        "variety": "ROBO",
        "tradingsymbol": "BANKNIFTY29DEC22FUT",
        "symboltoken": "62808",
        "transactiontype": transactiontype,
        # "transactiontype": "SELL",
        "exchange": "NFO",
        "ordertype": "MARKET",
        "producttype": "INTRADAY",
        "duration": "DAY",
        "price":"43300",
        "squareoff":squareoff,
        "stoploss":sl,
        "quantity":"25"
    }
    # print(orderparams)
    orderId = angle_smart_obj.placeOrder(orderparams)
    print("Conditions met and order executed",orderId)
    print("Entry:",entry)
    print("StopLoss:",sl)
    print("Target:",squareoff)
    print("Side:",side)

if __name__=="__main__":
    angle_smart_obj = angleSmartApiConnection()
    data = get30MinCandleData(angle_smart_obj)
    last3_candles_dict = preprocessCandlesData(data)
    entry, sl, side = strategy(last3_candles_dict)
    if side != 0:
        executeTrade(angle_smart_obj,entry, sl, side) 
    else:
        print("Conditions did not meet. So order not placed.")
    print("Script executed at:",datetime.datetime.now())
