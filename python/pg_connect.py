from sqlalchemy import create_engine
import psycopg2
import sys



def pg_connect(pg_password, database):
    conn = None
    try:
        engine = create_engine('postgresql://postgres:'+pg_password+'@localhost:5432/'+database+'?gssencmode=disable')
        
        # https://docs.sqlalchemy.org/en/13/core/connections.html#working-with-raw-dbapi-connections
        conn = engine.connect()
        #conn = connection.connection
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        # https://docs.python.org/3/library/sys.html#sys.exit
        #sys.exit(1) 
    print("Connection successful")
    return conn