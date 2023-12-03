from datetime import datetime
from psycopg2.extras import execute_values
from modules.json_open import open_connection_file
from modules.connect_db import engine_db
import pandas as pd
import os
import sys

class log_init():
    # Input for log init
    def __init__(self, name_script,connection=None,status_script=None):
        self.name_script=name_script
        self.status_script = status_script
        self.connection = connection if connection is not None else self.log_conn()

    # Log Connection
    def log_conn(self):
        current_directory = os.path.dirname(os.path.abspath(__file__))
        relative_path = os.path.join('..', 'credential', 'log_db.json')
        absolute_path = os.path.abspath(os.path.join(current_directory, relative_path))
        dbname_log,username_log,host_log,password_log,port_log,dbtype_log=open_connection_file(absolute_path)
        conn_log = engine_db(db_username=username_log,
                            db_host=host_log,
                            db_password=password_log,
                            db_name=dbname_log,
                            db_port=port_log,
                            db_type=dbtype_log
                            )
        return conn_log

    # Log structure
    def log(self,message_log,status_run,erors='None',final_run=False,total_row=0):
        tanggal2 = datetime.now()
        tgl = tanggal2.strftime('%Y-%m-%d %H:%M:%S')
        df_log = pd.DataFrame({'start_run':[tgl],
                               'command':[str(message_log)],
                               'eror':[str(erors)],
                               'total_export_row':[total_row],
                               'status_script':[self.status_script],
                               'status_run':[status_run],
                               'script_name':[self.name_script],
                               'final_run':[final_run]})
        df_log['start_run'] = pd.to_datetime(df_log['start_run'])
        insert_statement = f"INSERT INTO log_etl ({', '.join(df_log.columns)}) VALUES %s"
        cursor = self.connection.cursor()
        execute_values(cursor, insert_statement, [tuple(row) for row in df_log.values])
        self.connection.commit()
        print('{0}, error: {1}'.format(str(df_log['command'][0]),str(df_log['eror'][0])))
        if status_run.lower() == 'failed':
            sys.exit(1) 
        else:
            pass

