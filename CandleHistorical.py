import requests
import time
import datetime
from datetime import timezone
import numpy as np
import itertools
import time
import json
import pytz


class CandleHistorical:

    def __init__(self,conn,resolution,market_name,historicalTableName,mixedTableName):
        self.mixedTableName = mixedTableName
        self.historicalTableName = historicalTableName
        self.sqlConnection = conn
        self.resolution = resolution
        self.defaultStartDate = "2022-05-21"
        self.market_name = market_name

    def getHistoricalTrades(self,start_time,end_time):

        finalResult = []
        firstTime = end_time
        count=1
        while firstTime!=start_time:
            print("page number: " + str(count))
            print("start time: " + str(start_time) + ", end time: " + str(end_time))
            data = {
                "resolution": self.resolution,
                "start_time": start_time,
                "end_time": end_time
            }
            response = requests.get("https://ftx.com/api/markets/"+self.market_name+"/candles", params = data)
            result = response.json()["result"]
            if len(result)==0: 
                break
            finalResult.insert(0,result)
            firstTime = int(result[0]["time"]/1000)
            end_time = firstTime-self.resolution
            count+=1

        finalResult = list(itertools.chain(*finalResult))

        return finalResult

    def getMostRecentRecord(self,tableName):
        cursor = self.sqlConnection.cursor()
        cursor.execute("select * from " + tableName + " where time = (select max(time) from " + tableName +")")
        result = cursor.fetchone()
        
        recent = {}
        if result is not None:
            recent["time"]= int(result[1]/1000)
            recent["open"] = result[2]
            recent["close"] = result[3]
            recent["high"] = result[4]
            recent["low"] = result[5]
            recent["volume"] = result[6]

        return recent


    # if there is a previous record, add one to the start time requested
    # as the API will take the next start time after that
    # if the table is empty, use the defaultStartDate
    def getStartTime(self,resultTime):
        defaultStartTime = int(time.mktime(datetime.datetime.strptime(self.defaultStartDate, "%Y-%m-%d").timetuple()))
        return resultTime + self.resolution  if resultTime else defaultStartTime

    def insertHistoricalTradesToSQL(self,result,tableName):
        numRecords = len(result)
        cursor = self.sqlConnection.cursor()
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
            pair = "'"+self.market_name+"'::varchar(16)"
            exchange = "'ftx'::varchar(16)"
            res_secs = str(self.resolution)+"::bigint"
            is_streamed = "0::bit"
            valueString = ",".join([startTime,time,open,close,high,low,vol,pair,exchange,res_secs,is_streamed])
            queryString = "INSERT INTO " + tableName + " (startTime,time, open,close,high,low,volume,pair,exchange,res_secs,is_streamed) values (" +valueString + ") "
            cursor.execute(queryString)
            self.sqlConnection.commit()


    def updateSQL(self):

        lastStartRecordMixed = self.getMostRecentRecord(self.mixedTableName)
        lastStartRecordHistorical = self.getMostRecentRecord(self.historicalTableName)

        #print("last start record historical: " + str(lastStartRecordHistorical))
        lastStartTimeMixed = lastStartRecordMixed["time"] if lastStartRecordMixed else None
        lastStartTimeHistorical = lastStartRecordHistorical["time"] if lastStartRecordHistorical else None
        newStartTimeMixed = self.getStartTime(lastStartTimeMixed)
        newStartTimeHistorical = self.getStartTime(lastStartTimeHistorical)

        # we only want to fetch records once, so take the min of the start times and use that
        earlierStartTime = np.min([newStartTimeHistorical,newStartTimeMixed])
        lastTime = lastStartTimeMixed
        lastResult = lastStartRecordMixed
        end_time = int(time.mktime(datetime.datetime.now().timetuple()))

        # note if we are making requet less than 15 seconds after last request, end_time<start_time
        if end_time>earlierStartTime:
            result = self.getHistoricalTrades(earlierStartTime,end_time)
            
            if len(result)>0:
                startTimes = [int(r["time"]/1000) for r in result]
                
                # the following 5 lines of code should be easily replaced with np.where statements, 
                # but for some VERY strange reason it isn't picking up the values properly, even though
                # they are found by testing equality as implemented below. something to look into later
                startIndHistorical = -1
                startIndMixed = -1
                for ind1 in range(len(startTimes)):
                    if startTimes[ind1] == newStartTimeHistorical: startIndHistorical = ind1
                    if startTimes[ind1] == newStartTimeMixed: startIndMixed = ind1

                beforeTime = datetime.datetime.now()
                self.insertHistoricalTradesToSQL(result[startIndMixed:],self.mixedTableName)
                self.insertHistoricalTradesToSQL(result[startIndHistorical:],self.historicalTableName)
                afterTime = datetime.datetime.now()
                diffTime = afterTime - beforeTime
                lastTimeInd = np.where(startTimes == np.max(startTimes))[0][0]
                #print("startTimes: " +str(startTimes))
                lastResult = result[lastTimeInd]

        return lastResult

