import pandas as pd
from psycopg2.extras import execute_values

# Create table based on query
def create_table(connection_init,path_query):
    with open(path_query,'r') as files:
        query = files.read()
    connection_init.cursor().execute(query)
    connection_init.commit()

# Load dataframe
def upload_dataframe(connection_init,table_name,path_dataframe):
    df = pd.read_csv(path_dataframe)
    total_row = len(df)
    insert_statement = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES %s"
    cursor = connection_init.cursor()
    execute_values(cursor, insert_statement, [tuple(row) for row in df.values])
    connection_init.commit()
    print(f"Data successfully uploaded to table {table_name}")
    return total_row
    

    