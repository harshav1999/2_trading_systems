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
    from_ist_date = datetime.datetime.now() - datetime.timedelta(minutes=650)
    to_ist_date = datetime.datetime.now() + datetime.timedelta(days=1) 
    data = {}
    for each_stock_token in BANKNIFTY_FUT_TOKEN:
        historic_parms = {
            "exchange": "NFO",
            "symboltoken": each_stock_token,
            "interval": "THIRTY_MINUTE", 
            "fromdate": from_ist_date.strftime("%Y-%m-%d %H:%M"), #"2022-11-28 09:20", now.strftime("%m/%d/%Y, %H:%M:%S")
            "todate": to_ist_date.strftime("%Y-%m-%d %H:%M") #"2022-11-28 12:14"
            }
        d = angle_smart_obj.getCandleData(historic_parms)
        data[each_stock_token] = d
        # data[each_stock_token] 
        # data[each_stock_symbol] = fyers.history(request_json) 
        # print("-----------------------------") 
    return data

def insert30MinCandleData(data):
    conn = mysql.connector.connect(
        host = HOST, 
        user = USER,
        passwd = PASSWORD,
        port=3306,
        database =DATABASE
        )  
    cursor = conn.cursor()  

    for eachKey in data:
        if data[eachKey]['message'] == 'SUCCESS':
            candle_data_list = data[eachKey]['data']
            preprocessed_data_list = []
            for each_candle in candle_data_list:
                temp_tuple = ("BankNiftyFut",eachKey, each_candle[0],'30min',
                            each_candle[1],
                            each_candle[2],
                            each_candle[3],
                            each_candle[4])
                preprocessed_data_list.append(temp_tuple)
        query = """
        insert ignore into candle_data
        (symbol, symboltoken, candle_datetime, candle_timeframe, open, high, low,close)
        values  (%s, %s,%s, %s,%s, %s,%s, %s) 
        """ 
        cursor.executemany(query, preprocessed_data_list)
    conn.commit()
    conn.close()


if __name__=="__main__":
    angle_smart_obj = angleSmartApiConnection()
    data = get30MinCandleData(angle_smart_obj)
    insert30MinCandleData(data)