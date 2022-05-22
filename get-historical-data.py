import requests
import time
import datetime
import psycopg2
from websockets import WebSocketClientProtocol
import asyncio
import numpy as np
import itertools
import websocket
import _thread
import time
import json

tableNameMixed = "highres_mixed"
tableNameHist = "highres_historical"
tableNameStream = "highres_stream"
defaultStartDate = "2022-05-18"

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
    
    finalResult = []
    firstTime = end_time
    count=1
    while firstTime!=start_time:
        print("page number: " + str(count))
        print("start time: " + str(start_time) + ", end time: " + str(end_time))
        data = {
            "resolution": resolution,
            "start_time": start_time,
            "end_time": end_time
        }
        response = requests.get("https://ftx.com/api/markets/"+market_name+"/candles", params = data)
        
        result = response.json()["result"]
        if len(result)==0: break
        print("length of result: " + str(len(result)))
        print("result[0]: " + str(result[0]))
        finalResult.insert(0,result)
        firstTime = convertOutTimeToInTime(result[0]["time"])
        print("first time: " + str(firstTime))
        end_time = firstTime-resolution
        count+=1

    finalResult = list(itertools.chain(*finalResult))

    #print("result: " + str(result))
    return finalResult

def convertOutTimeToInTime(sqlTime):
    return int(sqlTime/1000)

def getMostRecentTimestamp(conn,tableName):
    cursor = conn.cursor()
    cursor.execute("select max(time) from " + tableName)
    result = cursor.fetchone()[0]
    print("result: " + str(result))
    resultTime = None
    if result is not None:
        resultTime = convertOutTimeToInTime(result)
    #print("resultTime: " + str(resultTime))
    return resultTime


# if there is a previous record, add one to the start time requested
# as the API will take the next start time after that
# if the table is empty, use the defaultStartDate
def getStartTime(resultTime,resolution):
     defaultStartTime = int(time.mktime(datetime.datetime.strptime(defaultStartDate, "%Y-%m-%d").timetuple()))
     return resultTime + resolution  if resultTime else defaultStartTime

def insertHistoricalTradesToSQL(conn,result,tableName):
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

        # insert into table populated by historical API + streaming API
        valueString = ",".join([startTime,time,open,close,high,low,vol])
        queryString = "INSERT INTO " + tableName + " (startTime,time, open,close,high,low,volume) values (" +valueString + ") "
        cursor.execute(queryString)
        conn.commit()

def insertStreamingTradesToSQL(conn,result,tableName):
    numRecords = len(result)
    cursor = conn.cursor()
    print("numRecords: " + str(numRecords))
    count=1
    for i in range(numRecords):
        row = result[i]
        tradeTime = row["time"].split("T")
        startDate = tradeTime[0]
        startTime = tradeTime[1].split("+")[0]
        parseTradeTime = time.mktime(datetime.datetime.strptime(startDate+" " + startTime, "%Y-%m-%d %H:%M:%S.%f").timetuple())
        price = str(row["price"])+"::decimal(32)"
        size = str(row["size"])+"::decimal(32)"
        side = str(row["side"])+"::varchar(4)"
        print("parseTradeTime: " + str(parseTradeTime) +", price: " + str(price) + ", size: " + str(size))
        #print("count: " + str(count))
        count += 1
        
        # # insert into table populated by historical API + streaming API
        # valueString = ",".join([startTime,time,open,close,high,low,vol])
        # queryString = "INSERT INTO " + tableName + " (startTime,time, open,close,high,low,volume) values (" +valueString + ") "
        # cursor.execute(queryString)
        # conn.commit()


def updateSQL(conn,resolution,market_name):

    # note the +1 will never move it to the next candle
    # because it only adds 1 second and the min resolution is 15 seconds

    lastStartTimeMixed = getMostRecentTimestamp(conn,tableNameMixed)
    lastStartTimeHistorical = getMostRecentTimestamp(conn,tableNameHist)

    newStartTimeMixed = getStartTime(lastStartTimeMixed,resolution = resolution)
    newStartTimeHistorical = getStartTime(lastStartTimeHistorical,resolution=resolution)

    print("newStartTimeMixed: " + str(newStartTimeMixed) + ", newStartTimeHistorical: " + str(newStartTimeHistorical))

    # we only want to fetch records once, so take the min of the start times and use that
    earlierStartTime = np.min([newStartTimeHistorical,newStartTimeMixed])
    result = getHistoricalTrades(market_name,resolution,earlierStartTime)
    lastTime = lastStartTimeMixed
    if len(result)>0:
        print("length of result: " + str(len(result)) )
        startTimes = [convertOutTimeToInTime(r["time"]) for r in result]

        startIndMixed = np.where(startTimes==earlierStartTime)[0][0]
        startIndHistorical = np.where(startTimes==earlierStartTime)[0][0]

        print("startIndMixed: " +str(startIndMixed) + ", startIndHistorical: " + str(startIndHistorical))
        insertHistoricalTradesToSQL(conn,result[startIndMixed:],tableNameMixed)
        insertHistoricalTradesToSQL(conn,result[startIndHistorical:],tableNameHist)
        lastTimeInd = np.where(startTimes == np.max(startTimes))[0][0]
        #print("startTimes: " +str(startTimes))
        lastResult = result[lastTimeInd]
        #print("lastResult: " + str(lastResult))

    return lastResult

class CandleSocket:
    
    def __init__(self, lastResult):
        self.lastStartTime = lastResult["time"]
        self.lastOpen = lastResult["open"]
        self.lastClose = lastResult["close"]
        self.lastHigh = lastResult["high"]
        self.lastLow = lastResult["low"]
        self.lastVolume = lastResult["volume"]
    # Define WebSocket callback functions

    def ws_message(self,ws, message):
        message = json.loads(message)
        if message["type"]=="update":
            insertStreamingTradesToSQL(conn,message["data"],tableNameStream)

    def ws_open(self,ws):
        #print("opening websocket")
        openString = '{"op": "subscribe", "channel": "trades", "market": "BTC-PERP"}'
        ws.send(openString)

    def on_error(self,ws, err):
        print("error encountered: ", err)

    def ws_thread(self):
        ws = websocket.WebSocketApp("wss://ftx.com/ws/", on_open = self.ws_open, on_message = self.ws_message, on_error = self.on_error)
        print("websocket object: "  + str(dir(ws)))
        ws.run_forever(ping_interval=15,ping_timeout=10)
        
    def run(self):
        while True:
            self.ws_thread()


# Continue other (non WebSocket) tasks in the main thread

async def consumer_handler(websocket: WebSocketClientProtocol) -> None:
    print("running consumer handler")
    async for message in websocket:
        print("message: " + str(message))

async def consumer() -> None:
    async with websockets.connect("wss://ws.kraken.com/") as websocket:
        await consumer_handler(websocket)

# if __name__ == "main":
#     asyncio.run(consumer())
conn = connectPSQL()

lastResult = updateSQL(conn, resolution = 15,market_name ="BTC-PERP")
print("last timestamp from historical update: " + str(lastResult["time"]))
wsCandles = CandleSocket(lastResult)
while True:
    wsCandles.run()
conn.close()