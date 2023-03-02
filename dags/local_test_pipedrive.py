import dlt
from pipedrive import pipedrive_source


if __name__ == "__main__" :
    # run our main example
    # load_pipedrive()
    # load selected tables and display resource info
    pipeline = dlt.pipeline(pipeline_name='pipedrive2', destination='bigquery', dataset_name='pipedrive_raw')

    load_info = pipeline.run(pipedrive_source())
    print(load_info)

    pipeline = dlt.pipeline(pipeline_name='pipedrive2', destination='bigquery', dataset_name='pipedrive_dbt')
    # now that data is loaded, let's transform it
    # make or restore venv for dbt, uses latest dbt version
    venv = dlt.dbt.get_venv(pipeline)
    # get runner, optionally pass the venv
    dbt = dlt.dbt.package(pipeline,
        "pipedrive/dbt_pipedrive/pipedrive",
        venv=venv)
    models = dbt.run_all()
    for m in models:
        print(f"Model {m.model_name} materialized in {m.time} with status {m.status} and message {m.message}")

