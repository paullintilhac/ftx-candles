from CandleSocket import CandleSocket
from CandleHistorical import CandleHistorical

import psycopg2
import asyncio

def connectPSQL():
        conn = psycopg2.connect(database="ftxtest", host='127.0.0.1')
        #Creating a cursor object using the cursor() method
        cursor = conn.cursor()
        cursor.execute("select version()")
        # Fetch a single row using fetchone() method.
        data = cursor.fetchone()
        cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';")
        tables = cursor.fetchone()
        return conn


conn = connectPSQL()

historicalTableName = "hist"
mixedTableName = "mixed"
resolution = 15
historicalCandles = CandleHistorical(conn,
                                    resolution = resolution,
                                    market_name = "BTC-PERP",
                                    historicalTableName=historicalTableName,
                                    mixedTableName = mixedTableName
                                    )

lastResult = historicalCandles.updateSQL()
cursor = conn.cursor()
cursor.execute("select max(startTime) from " + mixedTableName)
resultTemp = cursor.fetchone()[0]
print("last timestamp from sql query: " + str(resultTemp))
print("last timestamp from historical update: " + str(lastResult["time"]))

wsCandles = CandleSocket(lastResult,resolution,conn,mixedTableName,"BTC-PERP")
asyncio.run(wsCandles.consumer())
