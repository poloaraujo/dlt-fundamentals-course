import dlt
from dlt.sources.helpers import requests
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth

# Create the following directory: lesson-3/.dlt
# Create a fille called secrets.toml inside .dlt and add the following:
    # [sources]
    # access_token = "your_access_token"
# Make sure you create a .gitignore file with .dlt in it

source_access_token = dlt.secrets["sources.access_token"]

@dlt.source
def github_source(access_token=source_access_token):
    client = RESTClient(
            base_url="https://api.github.com",
            auth=BearerTokenAuth(token=access_token)
    )

    @dlt.resource
    def github_events():
        for page in client.paginate("orgs/dlt-hub/events"):
            yield page


    @dlt.resource
    def github_stargazers():
        for page in client.paginate("repos/dlt-hub/dlt/stargazers"):
            yield page

    return github_events, github_stargazers

# define new dlt pipeline
secrets_pipeline = dlt.pipeline(
    pipeline_name="secrets_pipeline",
    destination="duckdb",
    dataset_name="github_data"
)

# run the pipeline with the new resource
load_info = secrets_pipeline.run(github_source())
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

select_all_tables(secrets_pipeline)