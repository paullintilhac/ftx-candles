import requests
import time
import datetime
import psycopg2
from websockets import WebSocketClientProtocol
import asyncio
import numpy as np
import itertools

tableName1 = "highres_mixed"
tableName2 = "highres_historical"
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
        print("length of result: " + str(len(result)))
        print("result[0]: " + str(result[0]))
        finalResult.insert(0,result)
        firstTime = convertSQLTimeToFTXTime(result[0]["time"])
        print("first time: " + str(firstTime))
        end_time = firstTime-resolution
        count+=1

    finalResult = list(itertools.chain(*finalResult))

    #print("result: " + str(result))
    return finalResult

def convertSQLTimeToFTXTime(sqlTime):
    return int(sqlTime/1000)

def getMostRecentTimestamp(conn,tableName):
    cursor = conn.cursor()
    cursor.execute("select max(time) from " + tableName)
    result = cursor.fetchone()[0]
    print("result: " + str(result))
    resultTime = None
    if result is not None:
        resultTime = convertSQLTimeToFTXTime(result)
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

# asyncio.run(consumer())

conn = connectPSQL()

# note the +1 will never move it to the next candle
# because it only adds 1 second and the min resolution is 15 seconds

lastStartTimeMixed = getMostRecentTimestamp(conn,tableName1)
lastStartTimeHistorical = getMostRecentTimestamp(conn,tableName2)

newStartTimeMixed = getStartTime(lastStartTimeMixed,resolution = 15)
newStartTimeHistorical = getStartTime(lastStartTimeHistorical,resolution=15)

print("newStartTimeMixed: " + str(newStartTimeMixed) + ", newStartTimeHistorical: " + str(newStartTimeHistorical))

# we only want to fetch records once, so take the min of the start times and use that
earlierStartTime = np.min([newStartTimeHistorical,newStartTimeMixed])
result = getHistoricalTrades("BTC-PERP",15,earlierStartTime)

print("length of result: " + str(len(result)) )
print("len of result[0]: " + str(len(result[0])))
startTimes = [convertSQLTimeToFTXTime(r["time"]) for r in result]

print("length of startTimes: " + str(len(startTimes)) + ", startTimes[0]: " + str(startTimes[0]) +", startTimes[len(startTimes)-1]: "+str(startTimes[len(startTimes)-1]))
startIndMixed = np.where(startTimes==earlierStartTime)[0][0]
startIndHistorical = np.where(startTimes==earlierStartTime)[0][0]

print("startIndMixed: " +str(startIndMixed) + ", startIndHistorical: " + str(startIndHistorical))
insertHistoricalTradesToSQL(conn,result[startIndMixed:],tableName1)
insertHistoricalTradesToSQL(conn,result[startIndHistorical:],tableName2)

conn.close()