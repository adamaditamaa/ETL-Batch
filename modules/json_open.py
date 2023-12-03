import json

# To open and declare every var on json file
def open_connection_file(file_path):
    with open(file_path,'r') as cn:
        data = cn.read()
    config = json.loads(data)
    db_name = config.get('db_name')
    username = config.get('username')
    host = config.get('host')
    password = config.get('password')
    port = config.get('port')
    dbtype = config.get('db_type')
    
    return db_name,username,host,password,port,dbtype