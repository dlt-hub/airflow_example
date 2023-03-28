import dlt
from pipedrive import pipedrive_source




def replace_umlauts_in_dict_keys(d):
    """
    Replaces umlauts in dictionary keys with standard characters.
    """
    umlaut_map =  {'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'ß': 'ss', 'Ä': 'Ae', 'Ö': 'Oe', 'Ü': 'Ue'}
    result = {}
    for k, v in d.items():
        new_key = ''.join(umlaut_map.get(c, c) for c in k)
        if isinstance(v, dict):
            result[new_key] = replace_umlauts_in_dict_keys(v)
        else:
            result[new_key] = v
    return result


if __name__ == "__main__" :
    # run our main example
    # load_pipedrive()
    # load selected tables and display resource info
    pipeline = dlt.pipeline(pipeline_name='pipedrive', destination='bigquery', dataset_name='rename_pipedrive_test')
    load_info = pipeline.run(pipedrive_source().dealFields)
    print(load_info)
    load_info = pipeline.run(pipedrive_source().deals.add_map(replace_umlauts_in_dict_keys))
    print(load_info)



"""
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
"""
