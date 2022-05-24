from CandleSocket import CandleSocket
from CandleHistorical import CandleHistorical
from PostgresConnection import PostgresConnection

import asyncio

historicalTableName = "hist"
mixedTableName = "mixed"
resolution = 15
print("updating historical candles...")

PG = PostgresConnection()

historicalCandles = CandleHistorical(PG.conn,
                                    resolution = resolution,
                                    market_name = "BTC-PERP",
                                    historicalTableName=historicalTableName,
                                    mixedTableName = mixedTableName
                                    )

lastResult = historicalCandles.updateSQL()

print("updating diff table after syncing historical data with latest streamed data...")
PG.updateDiffTable()

wsCandles = CandleSocket(lastResult,resolution,PG.conn,mixedTableName,"BTC-PERP")
asyncio.run(wsCandles.consumer())
