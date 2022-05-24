from CandleSocket import CandleSocket
from CandleHistorical import CandleHistorical
from PostgresConnection import PostgresConnection

import asyncio

market_name = "BTC-PERP"
resolutions = [60,3600,86400]

print("updating historical candles...")
PG = PostgresConnection()
historicalCandles = CandleHistorical(PG.conn,resolutions = resolutions,market_name = market_name )
lastResults = historicalCandles.updateSQL()

print("updating diff table after syncing historical data with latest streamed data...")
PG.updateDiffTable()

print("beginning data stream...")
wsCandles = CandleSocket(lastResults,resolutions,PG.conn,market_name=market_name)
asyncio.run(wsCandles.consumer())
