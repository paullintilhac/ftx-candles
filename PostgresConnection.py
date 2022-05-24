import psycopg2
class PostgresConnection():
    def __init__(self,database="ftxtest",host ='127.0.0.1'):
        self.conn = psycopg2.connect(database=database, host=host)
        #Creating a cursor object using the cursor() method
        cursor = self.conn.cursor()
        cursor.execute("select version()")
        # Fetch a single row using fetchone() method.
        data = cursor.fetchone()
        cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';")
        tables = cursor.fetchone()

    def updateDiffTable(self):
        cursor = self.conn.cursor()
        cursor.execute("""
        insert into diff 
        select 
        hist.starttime,
        hist.time,
        mixed.open-hist.open as open_diff,
        mixed.close-hist.close as close_diff,
        mixed.high -hist.high as high_diff,
        mixed.low - hist.low as low_diff,
        mixed.volume - hist.volume as vol_diff,
        hist.exchange,
        hist.pair,
        hist.res_secs
        from hist inner join mixed on
        hist.time=mixed.time and 
        hist.exchange = mixed.exchange and 
        hist.pair = mixed.pair and
        hist.res_secs = mixed.res_secs
        where mixed.is_streamed =B'1'
        on conflict on constraint diff_id do nothing
        -- where mixed.time > (select max(time) from hist) OR
        -- (select max(time) from hist) is null

        """)
