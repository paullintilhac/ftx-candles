from websockets import WebSocketClientProtocol
import websockets
import json
import pytz
import time
from datetime import timezone
import datetime
import numpy as np
import requests
class CandleSocket:
    
    def __init__(self, lastResults,resolutions,conn,market_name,mixedTableName="mixed"):
        self.currentStartTimes = []
        self.currentOpens = []
        self.currentCloses = []
        self.currentHighs = []
        self.currentLows = []
        self.currentVolumes = []
        for i in range(len(lastResults)):
            self.currentStartTimes.append(int(lastResults[i]["time"]/1000))
            #print("current start datetime:" + str(lastResults[i]["startTime"]))
            self.currentOpens.append(lastResults[i]["open"])
            self.currentCloses.append(lastResults[i]["close"])
            self.currentHighs.append(lastResults[i]["high"])
            self.currentLows.append(lastResults[i]["low"])
            self.currentVolumes.append(0)
        self.resolutions = resolutions
        self.sqlConnection = conn
        self.mixedTableName = mixedTableName
        self.market_name = market_name
    # Define WebSocket callback functions

    async def ws_message(self, message):
        message = json.loads(message)
        if message["type"]=="update":
            result = message["data"]
            numRecords = len(result)
            cursor = self.sqlConnection.cursor()
            uniqueTPVols = {}
            for i in range(numRecords):
                row = result[i]
                timePrice = (row["time"],row["price"])
                if timePrice not in uniqueTPVols.keys():
                    uniqueTPVols[timePrice] = 0
                uniqueTPVols[timePrice] += float(row["size"]*row["price"])

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
            # so that we can process them properly in case they cross a candle boundary
            sortedInds = np.argsort(uniqueTimes)
            sortedTimes = [uniqueTimes[idx] for idx in sortedInds]
            sortedPrices = [uniquePrices[idx] for idx in sortedInds]
            sortedVols = [vols[idx] for idx in sortedInds] 

            
            for k in range(len(self.resolutions)):
                for j in range(len(sortedTimes)):
                    #print("sortedTimes[j] " + str(sortedTimes[j]) +", current start time: " + str(self.currentStartTimes[k]) + ", res: " + str(self.resolutions[k]))
                    intervalsAhead = int((sortedTimes[j] - self.currentStartTimes[k] )//self.resolutions[k])
                    # if we have reached a new time interval n ahead of the last closed one,
                    # consider the intervening n-1 intervals to be closed. Note this is a "lazy"
                    # method for generating new candles which avoids needing to use a separate
                    # process for generating intervals. If there are always trades in every interval
                    # it should be equivalent
                    
                    newTickDatetime = None
                    finalClose = self.currentCloses[k]
                    finalOpen = self.currentOpens[k]
                    finalVol = self.currentVolumes[k]
                    finalLow = self.currentLows[k]
                    finalHigh = self.currentHighs[k]

                    #print("intervals ahead: " + str(intervalsAhead))
                    if intervalsAhead>0:
                        response = requests.get("https://ftx.com/api/futures/"+self.market_name+"/stats")
                        result = response.json()["result"]
                        openInterest = float(result["openInterest"])
                        print("open interest at close of new "+ str(self.resolutions[k])+" sec bar: " + str(openInterest))
                        finalOpenInterest = openInterest

                        for i in range(intervalsAhead):
                            # if we are only closing one bar, i.e. we did not "skip ahead" at all,
                            # then us the current ohlc info for this bar before saving
                            # however, if we did skip ahead, we use the close of the first closed bar
                            # as the high, low, and open of the subsequent skipped bars, and vol=0
                            
                            if i>0: 
                                finalOpen = finalClose
                                finalHigh = finalClose
                                finalLow = finalClose
                                finalVol = 0
                                finalOpenInterest = 0
                            imputedStartTime = int(self.currentStartTimes[k] + self.resolutions[k]*i)
                            # write records to sql
                            startTimestamp = datetime.datetime.fromtimestamp(imputedStartTime, datetime.timezone.utc)
                            
                            startTimeString = "'"+str(startTimestamp)+"'"
                            if i==intervalsAhead-1:
                                newTickDatetime = startTimeString
                            timeString = str(imputedStartTime*1000)+"::bigint"
                            openString = str(finalOpen)+"::decimal(32,8)"
                            closeString = str(finalClose)+"::decimal(32,8)"
                            highString = str(finalHigh)+"::decimal(32,8)"
                            lowString = str(finalLow)+"::decimal(32,8)"
                            volString = str(finalVol)+"::decimal(32,8)"
                            pairString = "'"+self.market_name+"'::varchar(16)"
                            exchangeString = "'ftx'::varchar(16)"
                            resSecString = str(self.resolutions[k]) + "::bigint"
                            isStreamedString = "1::bit"
                            openInterestString = str(openInterest) + "::decimal(32,8)"
                            # insert into table populated by historical API + streaming API
                            valueString = ",".join([startTimeString,
                                                timeString,
                                                openString,
                                                closeString,
                                                highString,
                                                lowString,
                                                volString,
                                                pairString,
                                                exchangeString,
                                                resSecString,
                                                isStreamedString,
                                                openInterestString])
                            queryString = "INSERT INTO " + self.mixedTableName + \
                            " (startTime,time, open,close,high,low,volume,pair,exchange,res_secs,is_streamed,open_interest) values (" \
                            +valueString + ") on conflict on constraint mixed_id do update "\
                            " set close = Excluded.close, high=excluded.high, low=excluded.low, volume = Excluded.volume, is_streamed=B'1'"
                            #print("query string: " + queryString)
                            cursor = self.sqlConnection.cursor()
                            cursor.execute(queryString)
                            self.sqlConnection.commit()

                        print("new candle at "+ str(newTickDatetime) +", closing previous " + str(intervalsAhead) + " candles.")
                        self.currentOpens[k] = sortedPrices[j]
                        self.currentVolumes[k] = 0
                        self.currentStartTimes[k] = self.currentStartTimes[k] + self.resolutions[k]*intervalsAhead

                    self.currentCloses[k] = sortedPrices[j]
                    if sortedPrices[j] > self.currentHighs[k]: 
                        self.currentHighs[k] = sortedPrices[j]
                    if sortedPrices[j] < self.currentLows[k]:
                        self.currentLows[k] = sortedPrices[j]
                    self.currentVolumes[k] += sortedVols[j]

                # now stream data into current incomplete bar. 
                # note openInterest request is excluded until bar closes for efficiency
                currentTime = int(self.currentStartTimes[k]) 
                startTimestamp = datetime.datetime.fromtimestamp(currentTime, datetime.timezone.utc)
                
                startTimeString = "'"+str(startTimestamp)+"'"
                timeString = str(currentTime*1000)+"::bigint"
                openString = str(self.currentOpens[k])+"::decimal(32,8)"
                highString = str(self.currentHighs[k])+"::decimal(32,8)"
                lowString = str(self.currentLows[k])+"::decimal(32,8)"
                volString = str(self.currentVolumes[k])+"::decimal(32,8)"
                pairString = "'"+self.market_name+"'::varchar(16)"
                exchangeString = "'ftx'::varchar(16)"
                resSecString = str(self.resolutions[k]) + "::bigint"
                isStreamedString = "1::bit"
                # insert into table populated by historical API + streaming API
                valueString = ",".join([startTimeString,
                                    timeString,
                                    openString,
                                    highString,
                                    lowString,
                                    volString,
                                    pairString,
                                    exchangeString,
                                    resSecString,
                                    isStreamedString])
                queryString = "INSERT INTO " + self.mixedTableName + \
                " (startTime,time, open,high,low,volume,pair,exchange,res_secs,is_streamed) values (" \
                +valueString + ") on conflict on constraint mixed_id do update "\
                " set high=excluded.high, low=excluded.low, volume = Excluded.volume, is_streamed=B'1'"
                #print("query string: " + queryString)
                cursor = self.sqlConnection.cursor()
                cursor.execute(queryString)
                self.sqlConnection.commit()

    async def consumer(self) -> None:
        async with websockets.connect("wss://ftx.com/ws/") as websocket:
            await websocket.send(json.dumps({
                "op":"subscribe",
                "channel":"trades",
                "market":self.market_name
            }))
            
            async for message in websocket:
                await self.ws_message(message)


    async def run_async(self):
        await asyncio.run(consumer())

