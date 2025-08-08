import requests
import dlt

def get_dlt_repos():
    url = "https://api.github.com/orgs/dlt-hub/repos"
    response = requests.get(url)
    response.raise_for_status()
    repos_response = response.json()[0]
    return repos_response

@dlt.resource
def dlt_events_resource():
    url = f"https://api.github.com/orgs/dlt-hub/events"
    response = requests.get(url)
    yield response.json()

@dlt.resource
def dlt_repos_resource():
    url = f"https://api.github.com/orgs/dlt-hub/repos"
    response = requests.get(url)
    yield response.json()


# Create a source with all resources
@dlt.source
def all_data_source():
   return dlt_events_resource, dlt_repos_resource

# Create pipeline
github_pipeline = dlt.pipeline(
    pipeline_name="github_pipeline",
    destination="duckdb",
    dataset_name="github_data"
)

# Run pipeline
load_info = github_pipeline.run(all_data_source())
print(load_info)

# Query a single table from the pipeline. In this case dlt_repos_resource
github_pipeline.dataset().dlt_events_resource.df()
print("\n")
print(f"Data from {dlt_repos_resource }- {github_pipeline.pipeline_name}")
dataset = github_pipeline.dataset(dataset_type="default").dlt_repos_resource.df()
print(dataset)

#### Build a transformer ####

#Create a resource for github repos
@dlt.resource
def github_repos_resource():
    url = f"https://api.github.com/orgs/dlt-hub/repos"
    response = requests.get(url)
    yield from response.json()

@dlt.resource
def stargazers_resource():
    url = f"https://api.github.com/repos/dlt-hub/rasa_semantic_schema/stargazers"
    response = requests.get(url)
    yield from response.json()

@dlt.transformer(data_from=github_repos_resource, table_name='stargazer_repos', write_disposition="replace")
def stargazers_repos_transformer(data_item):
    owner = "dlt-hub"
    repo = data_item["name"]
    url = f"https://api.github.com/repos/{owner}/{repo}/stargazers"

    response = requests.get(url)

    if response.status_code != 200:
        yield {"error": response.json(), "repo": repo, "owner": owner}
        return

    # You get a list of dictionaries. Iterate through the list and add columns repo and owner
    for user in response.json():
        #user["repo"] = repo
        #user["owner"] = owner
        yield user


# Pipeline for github repos
github_repos_pipeline = dlt.pipeline(
   pipeline_name="github_repos_resource",
   destination="duckdb",
   dataset_name="dlt_stargazers"
)


# Pipeline for stargazer repos
stargazer_transformer_pipeline = dlt.pipeline(
   pipeline_name="stargazers_repos_transformer",
   destination="duckdb",
   dataset_name="dlt_stargazers",
   dev_mode=True
)

load_info = github_repos_pipeline.run(github_repos_resource())
print(load_info)

load_info = stargazer_transformer_pipeline.run(stargazers_repos_transformer())
print(load_info)



# This is a method that I wrote to go through all of the tables loaded by the pipeline. It is not part of the course
def select_all_tables(pipeline):
    # List all table names from the database
    with pipeline.sql_client() as client:
        table_names = []

        with client.execute_query("SELECT table_name FROM information_schema.tables") as table:
            print("\n")
            print(f"All tables loaded in {pipeline.pipeline_name}")

            tables_df = table.df()
            print(tables_df)

            table_names = []
            if tables_df is not None and not tables_df.empty:
                for _, row in tables_df.iterrows():
                    table_names.append(row["table_name"])
                print("Tables found:", table_names)
            else:
                print("⚠️ No tables found or query failed.")

    # Loop through each table name and fetch the data
    for table in table_names:
        print("\n")
        print(f"{pipeline.pipeline_name} - {table}")

        # Dynamically access the table
        dataset = getattr(pipeline.dataset(dataset_type="default"), table).df()
        print(dataset)

select_all_tables(github_repos_pipeline)
select_all_tables(stargazer_transformer_pipeline)
