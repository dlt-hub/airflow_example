from typing import List

import dlt

from sql_database.sql_database import sql_database, sql_table


def load_all():
    pipeline = dlt.pipeline(
        destination='bigquery',
        pipeline_name='sql22',
        dataset_name='sql_database_data',
        full_refresh=False
    )
    data = sql_database()
    print('will load:')
    print(data.resources.keys())
    info = pipeline.run(data)
    print(info)


def load_table(credentials, table, schema, incremental, write_disposition):
    pipeline = dlt.pipeline(
        destination='bigquery',
        pipeline_name='sql33',
        dataset_name='sql_database_data',
        full_refresh=False
    )
    data = sql_table(credentials=credentials, table=table, schema=schema, incremental=incremental, write_disposition=write_disposition)
    info = pipeline.run(data)
    print(info)

#database_url = "mysql+pymysql:///root:beetroot@localhost:3306/tst"



if __name__ == '__main__':
    host = "mysql-rfam-public.ebi.ac.uk"
    port = 4497
    database = "Rfam"
    user = 'rfamro'
    conn_string = f"mysql+pymysql:///{user}:@{host}:{port}/{database}"
    from sqlalchemy import create_engine
    engine = create_engine(conn_string)
    load_table(credentials=engine, table='dead_clan', schema=None, incremental=None, write_disposition='replace')



