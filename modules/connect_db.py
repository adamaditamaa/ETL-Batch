import psycopg2 as psy
import mysql.connector as cnn

# Create engine db to connect on database 
def engine_db(db_username,db_host,db_type,db_port,db_name,db_password):
    if db_type.lower() == 'postgresql':
        conn = psy.connect(user = db_username, dbname = db_name,
                            password = db_password, host = db_host, port = db_port)
    elif db_type.lower()=='mysql':
        conn = cnn.connect(host= str(db_host),
            user = str(db_username),
            password = str(db_password),
            database = str(db_name),
            port = db_port)
    return conn