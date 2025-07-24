import dlt
from dlt.sources.helpers import requests
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth


access_token = "your_access_token"

# define new resource - github stargazers
@dlt.resource
def github_stargazers():
    client = RESTClient(
        base_url="https://api.github.com",
        auth=BearerTokenAuth(token=access_token)
    )

    for page in client.paginate("repos/dlt-hub/dlt/stargazers"):
        yield page


# define new dlt pipeline
pipeline = dlt.pipeline(
    pipeline_name="paginated_pipeline",
    destination="duckdb",
    dataset_name="stargazers"
)

# run the pipeline with the new resource
load_info = pipeline.run(github_stargazers)
print(load_info)


# explore loaded data
print(pipeline.dataset().github_stargazers.df())