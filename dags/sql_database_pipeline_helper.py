from typing import List

import dlt
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.common import pendulum

from airflow.decorators import dag, task
from tenacity import wait_fixed, stop_after_attempt, Retrying

from sql_database import sql_database, sql_table
from helper import AirflowTasks

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def load__tables_from_database_helper() -> None:
    """Use the sql_database source to reflect an entire database schema and load select tables from it.

    This example sources data from the public Rfam MySQL database.
    """
    tasks_merge = AirflowTasks(
        "sql_load_merge",
        retry_policy=Retrying(stop=stop_after_attempt(2), wait=wait_fixed(5.0), reraise=True)
    )

    tasks_replace = AirflowTasks(
        "sql_load_replace",
        retry_policy=Retrying(stop=stop_after_attempt(2), wait=wait_fixed(5.0), reraise=True)
    )

    tasks_append = AirflowTasks(
        "sql_load_append",
        retry_policy=Retrying(stop=stop_after_attempt(2), wait=wait_fixed(5.0), reraise=True)
    )


    # Create a pipeline
    pipeline = dlt.pipeline(pipeline_name='rfam', destination='bigquery', dataset_name='rfam_data')

    # Credentials for the sample database.
    # Note: It is recommended to configure credentials in `.dlt/secrets.toml` under `sources.sql_database.credentials`
    credentials = ConnectionStringCredentials("mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam")

    # Configure the source to load a few select tables incrementally
    # Task Merge
    source_1 = sql_database(credentials).with_resources('family', 'clan')

    # Add incremental config to the resources. "updated" is a timestamp column in these tables that gets used as a cursor
    source_1.family.apply_hints(incremental=dlt.sources.incremental('updated'))
    source_1.clan.apply_hints(incremental=dlt.sources.incremental('updated'))

    # Run the pipeline. The merge write disposition merges existing rows in the destination by primary key
    tasks_merge.add_run(pipeline,  source_1, write_disposition='merge')

    # Task Append
    source_2 = sql_database(credentials).with_resources('features', "author")
    tasks_replace.add_run(pipeline,  source_2, write_disposition='replace')



    # Load a table incrementally with append write disposition
    # this is good when a table only has new rows inserted, but not updated
    source_3 = sql_database(credentials).with_resources('genome')
    source_3.genome.apply_hints(incremental=dlt.sources.incremental('created'))

    tasks_append.add_run(pipeline,source_3,write_disposition='append')


'''
def load_entire_database() -> None:
    """Use the sql_database source to completely load all tables in a database"""
    pipeline = dlt.pipeline(pipeline_name='rfam', destination='bigquery', dataset_name='rfam_data')

    # By default the sql_database source reflects all tables in the schema
    # The database credentials are sourced from the `.dlt/secrets.toml` configuration
    source = sql_database()

    # Run the pipeline. For a large db this may take a while
    info = pipeline.run(source, write_disposition="replace")
    print(info)


def load_standalone_table_resource() -> None:
    """Load a few known tables with the standalone sql_table resource"""
    pipeline = dlt.pipeline(pipeline_name='rfam_database', destination='bigquery', dataset_name='rfam_data')

    # Load a table incrementally starting at a given date
    # Adding incremental via argument like this makes extraction more efficient
    # as only rows newer than the start date are fetched from the table
    family = sql_table(
        table='family',
        incremental=dlt.sources.incremental('updated', initial_value=pendulum.DateTime(2022, 1, 1, 0, 0, 0))
    )

    # Load all data from another table
    genome = sql_table(table='genome')

    # Run the resources together
    info = pipeline.extract([family, genome], write_disposition='merge')
    print(info)



if __name__ == '__main__':
    # Load selected tables with different settings
    load_select_tables_from_database()

    # Load tables with the standalone table resource
    # load_standalone_table_resource()

    # Load all tables from the database.
    # Warning: The sample database is very large
    # load_entire_database()
'''
load__tables_from_database_helper()