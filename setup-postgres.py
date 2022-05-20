import psycopg2

def connectPSQL():
    conn = psycopg2.connect(database="ftxdata", host='127.0.0.1')
    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    cursor.execute("select version()")
    # Fetch a single row using fetchone() method.
    data = cursor.fetchone()
    print("Connection established to: ",data)
    cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';")
    tables = cursor.fetchone()
    print("tables: " + str(tables))
    conn.close()

connectPSQL()