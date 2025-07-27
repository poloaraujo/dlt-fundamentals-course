import dlt
from dlt.sources.helpers import requests
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

import os

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
    pipeline_name="github_pipeline1",
    destination="duckdb",
    dataset_name="github_data",
    export_schema_path="schemas/export", # <--- dir path for a schema export. This creates a very pretty yaml
    #dev_mode=True
)

def run_resource():
    # run the pipeline with the new resource
    load_info = pipeline.run(github_source())
    print(load_info)

    # You can inspect schema changes by running dlt pipeline -v github_pipeline1 load-package in the terminal
    # Alternatively, you can print the below, but note that the json will only be printed if you're
    # running the pipeline for the first time or you're that you need dev_mode to be true in the pipeline to
    # trigger a full refresh.
    try:
        first_package = load_info.load_packages[0]
        print(first_package.schema.to_pretty_json())
    except IndexError:
        print("⚠️ No load packages were created. The pipeline likely had nothing to load.")
    except Exception as e:
        print(f"⚠️ Unexpected error: {e}")

# Pipeline with imported schema
pipeline_imported = dlt.pipeline(
    pipeline_name="github_pipeline3",
    destination="duckdb",
    dataset_name="github_data",
    export_schema_path="schemas/export",
    import_schema_path="schemas/import",
)

def run_resource_imported():
    # run the pipeline with the new resource
    load_info = pipeline_imported.run(github_source())
    print(load_info)

    try:
        first_package = load_info.load_packages[0]
        print(first_package.schema.to_pretty_json())
    except IndexError:
        print("⚠️ No load packages were created. The pipeline likely had nothing to load.")
    except Exception as e:
        print(f"⚠️ Unexpected error: {e}")

if __name__ == "__main__":
    #run_resource()
    run_resource_imported()