import requests
import time
import datetime
from datetime import timezone
import psycopg2
from websockets import WebSocketClientProtocol
import websockets
import asyncio
import numpy as np
import itertools
import websocket
import _thread
import time
import json
import pytz

tableNameMixed = "mixed_15_sec"
tableNameHist = "historical_15_sec"
tableNameStream = "stream_15_sec"
defaultStartDate = "2022-05-21"

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

def getHistoricalTrades(market_name,resolution:int,start_time,end_time):

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
        print("response: " + str(response))
        #print(response.json())
        result = response.json()["result"]
        # print("result: " + str(result))
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

def getMostRecentRecord(conn,tableName):
    cursor = conn.cursor()
    cursor.execute("select * from " + tableName + " where time = (select max(time) from " + tableName +")")
    result = cursor.fetchone()
    
    print("result: " + str(result))
    recent = {}
    if result is not None:
        recent["time"]= convertOutTimeToInTime(result[1])
        recent["open"] = result[2]
        recent["close"] = result[3]
        recent["high"] = result[4]
        recent["low"] = result[5]
        recent["volume"] = result[6]

    #print("resultTime: " + str(resultTime))
    return recent


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

        time = str(int(row["time"]))+"::bigint"
        open = str(row["open"])+"::decimal(32,8)"
        close = str(row["close"])+"::decimal(32,8)"
        high = str(row["high"])+"::decimal(32,8)"
        low = str(row["low"])+"::decimal(32,8)"
        vol = str(row["volume"])+"::decimal(32,8)"

        valueString = ",".join([startTime,time,open,close,high,low,vol])
        queryString = "INSERT INTO " + tableName + " (startTime,time, open,close,high,low,volume) values (" +valueString + ") "
        cursor.execute(queryString)
        conn.commit()


def updateSQL(conn,resolution,market_name):

    lastStartRecordMixed = getMostRecentRecord(conn,tableNameMixed)
    lastStartRecordHistorical = getMostRecentRecord(conn,tableNameHist)

    lastStartTimeMixed = lastStartRecordMixed["time"] if lastStartRecordMixed else None
    lastStartTimeHistorical = lastStartRecordHistorical["time"] if lastStartRecordHistorical else None
    newStartTimeMixed = getStartTime(lastStartTimeMixed,resolution = resolution)
    newStartTimeHistorical = getStartTime(lastStartTimeHistorical,resolution=resolution)

    print("newStartTimeMixed: " + str(newStartTimeMixed) + ", newStartTimeHistorical: " + str(newStartTimeHistorical))

    # we only want to fetch records once, so take the min of the start times and use that
    earlierStartTime = np.min([newStartTimeHistorical,newStartTimeMixed])
    lastTime = lastStartTimeMixed
    lastResult = lastStartRecordMixed
    end_time = int(time.mktime(datetime.datetime.now().timetuple()))

    #note if we are making requet less than 15 seconds after last request, end_time<start_time
    if end_time>earlierStartTime:
        result = getHistoricalTrades(market_name,resolution,earlierStartTime,end_time)
        
        if len(result)>0:
            print("length of result: " + str(len(result)) )
            startTimes = [convertOutTimeToInTime(r["time"]) for r in result]
            print("earlierStartTime: " + str(earlierStartTime) + ", newStartTimeHistorical: " + str(newStartTimeHistorical) + ", equal? " + str(newStartTimeHistorical==earlierStartTime))
            
            # the following 5 lines of code should be easily replaced with np.where statements, 
            # but for some VERY strange reason it isn't picking up the values properly, even though
            # they are found by testing equality as implemented below. something to look into later
            startIndHistorical = -1
            startIndMixed = -1
            for ind1 in range(len(startTimes)):
                if startTimes[ind1] == newStartTimeHistorical: startIndHistorical = ind1
                if startTimes[ind1] == newStartTimeMixed: startIndMixed = ind1

            print("startIndMixed: " +str(startIndMixed) + ", startIndHistorical: " + str(startIndHistorical))
            beforeTime = datetime.datetime.now()
            insertHistoricalTradesToSQL(conn,result[startIndMixed:],tableNameMixed)
            insertHistoricalTradesToSQL(conn,result[startIndHistorical:],tableNameHist)
            afterTime = datetime.datetime.now()
            diffTime = afterTime - beforeTime
            print("time to insert historical trades into SQL: " + str(diffTime))
            lastTimeInd = np.where(startTimes == np.max(startTimes))[0][0]
            #print("startTimes: " +str(startTimes))
            lastResult = result[lastTimeInd]
            print("lastResult: " + str(lastResult))

    return lastResult

class CandleSocket:
    
    def __init__(self, lastResult,resolution,conn,tableName):
        self.currentStartTime = convertOutTimeToInTime(lastResult["time"])
        self.currentOpen = lastResult["close"]
        self.currentClose = lastResult["close"]
        self.currentHigh = lastResult["close"]
        self.currentLow = lastResult["close"]
        self.currentVolume = 0
        self.currentIntervalsAhead = 0
        self.resolution = resolution
        self.sqlConnection = conn
        self.tableName = tableName

    # Define WebSocket callback functions

    async def ws_message(self, message):
        message = json.loads(message)
        # print("message: " + str(message))
        if message["type"]=="update":
            result = message["data"]
            numRecords = len(result)
            print("numRecords: " + str(numRecords))
            cursor = conn.cursor()
            uniqueTPVols = {}
            for i in range(numRecords):
                row = result[i]
                timePrice = (row["time"],row["price"])
                if timePrice not in uniqueTPVols.keys():
                    uniqueTPVols[timePrice] = 0
                uniqueTPVols[timePrice] += float(row["size"])
            timezone = pytz.timezone('America/New_York')
            
            #get unique price time records into ordered arrays
            uniqueTimes = []
            uniquePrices = []
            vols = []
            
            
            for i in uniqueTPVols:
                uniqueTime = i[0]
                uniquePrices.append( i[1])
                vols.append(uniqueTPVols[i])
                uniqueTimes.append(time.mktime(datetime.datetime.strptime(uniqueTime, "%Y-%m-%dT%H:%M:%S.%f%z").astimezone(timezone).timetuple()))
            
            # get the aggregated records for each unique price-time sorted by time
            # so that we can process them properly in case the cross a candle boundary

            sortedInds = np.argsort(uniqueTimes)
            sortedTimes = [uniqueTimes[idx] for idx in sortedInds]
            sortedPrices = [uniquePrices[idx] for idx in sortedInds]
            sortedVols = [vols[idx] for idx in sortedInds] 

            for j in range(len(sortedTimes)):

                intervalsAhead = int((sortedTimes[j] - self.currentStartTime )//self.resolution)
                print("intervals ahead: " + str(int(intervalsAhead)))
                # if we have reached a new time interval ahead of the current one,
                # consider the intervening intervals to be closed. Note this is a "lazy"
                # method for generating new candles which avoids needing to use a separate
                # process for generating intervals. If there are always trades in every interval
                # it should be equivalent

                if intervalsAhead>0:
                    finalClose = self.currentClose
                    finalOpen = self.currentOpen
                    finalVol = self.currentVolume
                    finalLow = self.currentLow
                    finalHigh = self.currentHigh
                    
                    for i in range(intervalsAhead):
                        imputedStartTime = int(self.currentStartTime + self.resolution*i)
                        print("imputed start time: " + str(imputedStartTime) + ", open: " + str(finalOpen) + ", close: " + str(finalClose))
                        # write records to sql
                        startTimestamp = datetime.datetime.fromtimestamp(imputedStartTime, datetime.timezone.utc)
                        
                        startTimeString = "'"+str(startTimestamp)+"'"
                        #print("startTime: " + startTime)

                        timeString = str(imputedStartTime*1000)+"::bigint"
                        openString = str(self.currentOpen)+"::decimal(32,8)"
                        closeString = str(self.currentClose)+"::decimal(32,8)"
                        highString = str(self.currentHigh)+"::decimal(32,8)"
                        lowString = str(self.currentLow)+"::decimal(32,8)"
                        volString = str(self.currentVolume)+"::decimal(32,8)"

                        # insert into table populated by historical API + streaming API
                        valueString = ",".join([startTimeString,timeString,openString,closeString,highString,lowString,volString])
                        queryString = "INSERT INTO " + self.tableName + " (startTime,time, open,close,high,low,volume) values (" +valueString + ") on conflict (time) do nothing "
                        print("queryString: " + str(queryString))
                        cursor = self.sqlConnection.cursor()
                        cursor.execute(queryString)
                        self.sqlConnection.commit()

                    print("new candle, closing previous " + str(intervalsAhead) + " candles.")
                    self.currentOpen = sortedPrices[j]
                    self.currentStartTime = self.currentStartTime + self.resolution*intervalsAhead
                
                self.currentClose = sortedPrices[j]
                if sortedPrices[j] > self.currentHigh: 
                    self.currentHigh = sortedPrices[j]
                if sortedPrices[j] < self.currentLow:
                    self.currentLow = sortedPrices[j]
                self.currentVolume += sortedVols[j]



    async def consumer(self) -> None:
        async with websockets.connect("wss://ftx.com/ws/") as websocket:
            await websocket.send(json.dumps({
                "op":"subscribe",
                "channel":"trades",
                "market":"BTC-PERP"
            }))
            
            async for message in websocket:
                #print("message: " + str(message))
                await self.ws_message(message)


    async def run_async(self):
        asyncio.run(consumer())


# if __name__ == "main":
#     asyncio.run(consumer())
conn = connectPSQL()

res = 15
lastResult = updateSQL(conn, resolution = res,market_name ="BTC-PERP")
cursor = conn.cursor()
cursor.execute("select max(startTime) from " + tableNameMixed)
resultTemp = cursor.fetchone()[0]
print("last timestamp from sql query: " + str(resultTemp))
print("last timestamp from historical update: " + str(lastResult["time"]))

wsCandles = CandleSocket(lastResult,res,conn,tableNameMixed)
asyncio.run(wsCandles.consumer())
