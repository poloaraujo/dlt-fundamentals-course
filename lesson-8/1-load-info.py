import dlt
from dlt.sources.helpers import requests
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

import json

source_access_token = dlt.secrets["sources.access_token"]


@dlt.source
def github_source(secret_key=source_access_token):
    client = RESTClient(
            base_url="https://api.github.com",
            auth=BearerTokenAuth(token=secret_key),
            paginator=HeaderLinkPaginator(),
    )

    @dlt.resource
    def github_pulls(cursor_date=dlt.sources.incremental("updated_at", initial_value="2024-12-01")):
        params = {
            "since": cursor_date.last_value,
            "status": "open"
        }
        for page in client.paginate("repos/dlt-hub/dlt/pulls", params=params):
            yield page


    return github_pulls


# define new dlt pipeline
pipeline = dlt.pipeline(
    pipeline_name="github_load_info_pipeline",
    destination="duckdb",
    dataset_name="github_data",
)


# run the pipeline with the new resource
load_info = pipeline.run(github_source())
print(load_info)

# print human friendly trace information
print(pipeline.last_trace)

# print human friendly normalization information
print(pipeline.last_trace.last_normalize_info)

 # access row counts dictionary of normalize info
print(pipeline.last_trace.last_normalize_info.row_counts)

# print human friendly load information
print(pipeline.last_trace.last_load_info)

# Print the state.json
def read_state(filepath):
    with open(filepath, 'r') as file:
        data = json.load(file)
        pretty_json = json.dumps(data, indent=4)
        return pretty_json

my_filepath = 'Users/your_user'
print(read_state(f"/{my_filepath}/.dlt/pipelines/github_load_info_pipeline/state.json"))



@dlt.source
def github_modified_state_source(secret_key=source_access_token):
    client = RESTClient(
            base_url="https://api.github.com",
            auth=BearerTokenAuth(token=secret_key),
            paginator=HeaderLinkPaginator(),
    )

    @dlt.resource
    def github_pulls(cursor_date=dlt.sources.incremental("updated_at", initial_value="2024-12-01")):

        # Let's set some custom state information
        dlt.current.resource_state().setdefault("new_key", ["first_value", "second_value"]) # <--- new item in the state

        params = {
            "since": cursor_date.last_value,
            "status": "open"
        }
        for page in client.paginate("repos/dlt-hub/dlt/pulls", params=params):
            yield page


    return github_pulls


# define new dlt pipeline
pipeline = dlt.pipeline(
    pipeline_name="github_modified_state_source",
    destination="duckdb",
    dataset_name="github_data",
)

# run the pipeline with the new resource
load_info = pipeline.run(github_source())
print(load_info)