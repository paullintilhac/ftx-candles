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
    
    def __init__(self, lastResult,resolution,conn,mixedTableName,market_name):
        self.currentStartTime = int(lastResult["time"]/1000)
        self.currentOpen = lastResult["open"]
        self.currentClose = lastResult["close"]
        self.currentHigh = lastResult["high"]
        self.currentLow = lastResult["low"]
        self.currentVolume = 0
        self.currentIntervalsAhead = 0
        self.resolution = resolution
        self.sqlConnection = conn
        self.mixedTableName = mixedTableName
        self.market_name = "BTC-PERP"
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

            for j in range(len(sortedTimes)):

                intervalsAhead = int((sortedTimes[j] - self.currentStartTime )//self.resolution)
                # if we have reached a new time interval n ahead of the last closed one,
                # consider the intervening n-1 intervals to be closed. Note this is a "lazy"
                # method for generating new candles which avoids needing to use a separate
                # process for generating intervals. If there are always trades in every interval
                newTickDatetime = None
                # it should be equivalent
                if intervalsAhead>0:
                    response = requests.get("https://ftx.com/api/futures/"+self.market_name+"/stats")
                    result = response.json()["result"]
                    openInterest = float(result["openInterest"])
                    print("open interest at close of new bar: " + str(openInterest))
                    for i in range(intervalsAhead):
                        # if we are only closing one bar, i.e. we did not "skip ahead" at all,
                        # then us the current ohlc info for this bar before saving
                        # however, if we did skip ahead, we use the close of the first closed bar
                        # as the high, low, and open of the subsequent skipped bars, and vol=0
                        
                        finalClose = self.currentClose
                        finalOpen = self.currentOpen
                        finalVol = self.currentVolume
                        finalLow = self.currentLow
                        finalHigh = self.currentHigh
                        finalOpenInterest = openInterest
                        if i>0: 
                            finalOpen = finalClose
                            finalHigh = finalClose
                            finalLow = finalClose
                            finalVol = 0
                            finalOpenInterest = 0
                        imputedStartTime = int(self.currentStartTime + self.resolution*i)
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
                        resSecString = str(self.resolution) + "::bigint"
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
                        queryString = "INSERT INTO " + self.mixedTableName + " (startTime,time, open,close,high,low,volume,pair,exchange,res_secs,is_streamed,open_interest) values (" +valueString + ") on conflict (time) do nothing "
                        cursor = self.sqlConnection.cursor()
                        cursor.execute(queryString)
                        self.sqlConnection.commit()

                    print("new candle at "+ str(newTickDatetime) +", closing previous " + str(intervalsAhead) + " candles.")
                    self.currentOpen = sortedPrices[j]
                    self.currentVolume = 0
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
                "market":self.market_name
            }))
            
            async for message in websocket:
                await self.ws_message(message)


    async def run_async(self):
        await asyncio.run(consumer())

