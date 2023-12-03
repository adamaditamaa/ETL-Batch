from modules.connect_db import engine_db
from modules.log import log_init
from modules.exec_dataframe import create_table,upload_dataframe
from modules.json_open import open_connection_file
import os

# Load variable
try:
    # file path
    current_dir = os.getcwd()
    folder_cred = 'credential'
    folder_data_source = 'data_source'
    folder_query_add = 'query_ddl'
    target_db_cred = 'target_db.json'

    # Target table name on list
    #list query
    query_list = []
    
    for file in os.listdir(os.path.join(current_dir,folder_query_add)):
        if file != 'log_table_create.sql':    
            name,ext = file.split('.')
            query_list.append(name)

    # cred extract
    dbname_target,username_target,host_target,password_target,port_target,dbtype_target=open_connection_file(os.path.join(
        current_dir,folder_cred,target_db_cred))

    # log init
    log_def = log_init(name_script='ETL Users',status_script='ETL')
    log_def.log('Load Variable And Connection Success',status_run='Success')
except Exception as eror:
    log_def.log('Load Variable And Connection Failed',status_run='Failed',erors=eror)

# Target db conection init
try:
    conn_target = engine_db(db_username=username_target,
                        db_host=host_target,
                        db_password=password_target,
                        db_name=dbname_target,
                        db_port=port_target,
                        db_type=dbtype_target
                        )
    log_def.log('Connect to target DB Success',status_run='Success')
except Exception as eror:
    log_def.log('Connect to target DB Failed',status_run='Failed',erors=eror)

try:
    for select_table in query_list:
        try:
            totalrow = upload_dataframe(connection_init=conn_target,
                            table_name=select_table,
                            path_dataframe= os.path.join(current_dir,
                                                        folder_data_source,
                                                        select_table+'.csv'
                                                        )
                            )
        except:
            create_table(
                connection_init=conn_target,
                path_query=os.path.join(current_dir,
                                        folder_query_add,
                                        select_table+'.sql'
                                        )
            )
            totalrow= upload_dataframe(connection_init=conn_target,
                            table_name=select_table,
                            path_dataframe= os.path.join(current_dir,
                                                        folder_data_source,
                                                        select_table+'.csv'
                                                        )
                            )
        log_def.log(f'Load Dataframe {select_table} Success',status_run='Success',total_row=int(totalrow))
    log_def.log('All Dataframe Success',status_run='Success',final_run=True)
except Exception as eror:
    log_def.log('Load Dataframe Failed',status_run='Failed',final_run=True) 









