import requests
import time
import datetime
import psycopg2
from websockets import WebSocketClientProtocol
import asyncio

tableName = "highres5"
def connectPSQL():
    conn = psycopg2.connect(database="ftxtest", host='127.0.0.1')
    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    cursor.execute("select version()")
    # Fetch a single row using fetchone() method.
    data = cursor.fetchone()
    print("Connection established to: ",data)
    cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';")
    tables = cursor.fetchone()
    return conn

def getHistoricalTrades(market_name,resolution:int,start_time):

    end_time = int(time.mktime(datetime.datetime.now().timetuple()))
    print("start time: " + str(start_time) + ", end time: " + str(end_time))
    
    data = {
        "resolution": resolution,
        "start_time": start_time,
        "end_time": end_time
    }
    response = requests.get("https://ftx.com/api/markets/"+market_name+"/candles", params = data)
    
    result = response.json()["result"]
    #print("result: " + str(result))
    return result

def getMostRecentTimestamp(conn):
    cursor = conn.cursor()
    cursor.execute("select max(time) from " + tableName)
    result = cursor.fetchone()[0]
    # print("result: " + str(result))
    resultTime = int(result/1000)
    # print("resultTime: " + str(resultTime))
    return resultTime

def insertHistoricalTradesToSQL(conn,result):
    numRecords = len(result)
    cursor = conn.cursor()
    print("numRecords: " + str(numRecords))
    for i in range(numRecords):
        row = result[i]
        startDateTime = row["startTime"].split("T")
        startDate = startDateTime[0]
        startTime = startDateTime[1].split("+")[0]
        newDateTime = str(startDate) + " " + str(startTime)
        startTime = "to_timestamp('"+newDateTime+"', 'YYYY-MM-DD HH24:MI:SS')"
        print("startTime: " + startTime)
        time = str(int(row["time"]))+"::bigint"
        open = str(row["open"])+"::decimal(32)"
        close = str(row["close"])+"::decimal(32)"
        high = str(row["high"])+"::decimal(32)"
        low = str(row["low"])+"::decimal(32)"
        vol = str(row["volume"])+"::decimal(32)"
        valueString = ",".join([startTime,time,open,close,high,low,vol])
        queryString = "INSERT INTO highres5 (startTime,time, open,close,high,low,volume) values (" +valueString + ") if startTime> select max(startTime) from "+tableName 
        cursor.execute(queryString)
        conn.commit()

#asyncio.run(consumer())

conn = connectPSQL()

lastStartTime = getMostRecentTimestamp(conn)
result = getHistoricalTrades("BTC-PERP",15,lastStartTime)

insertHistoricalTradesToSQL(conn,result)


conn.close()