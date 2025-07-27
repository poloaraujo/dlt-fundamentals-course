import dlt
from dlt.sources.helpers import requests
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth

import duckdb

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

    #return github_events, github_stargazers
    return github_stargazers, github_events

# define new dlt pipeline
secrets_pipeline = dlt.pipeline(
    pipeline_name="secrets_pipeline",
    destination="duckdb",
    dataset_name="github_data",
)

# Run he pipeline for the source
def run_source():
    # run the pipeline with the new resource
    load_info = secrets_pipeline.run(github_source())
    print(load_info)

# This is a method that I wrote to go through all of the tables loaded by the pipeline. It is not part of the course
def select_all_tables(pipeline):
    # List all table names from the database
    with pipeline.sql_client() as client:
        table_names = []

        with client.execute_query("SELECT table_schema, table_name FROM information_schema.tables") as table:
            print(f"\nAll tables loaded in {pipeline.pipeline_name}")

            tables_df = table.df()
            print(tables_df)

            table_names = []

            if tables_df is not None and not tables_df.empty:
                table_names = [
                    (row["table_schema"], row["table_name"]) for _, row in tables_df.iterrows()
                ]
                print("\nTables found:", [t[1] for t in table_names])
            else:
                print("⚠️ No tables found or query failed.")

    # Query each table with fully qualified names
    with pipeline.sql_client() as client:
        for schema, table in table_names:
            print(f"\n{pipeline.pipeline_name} - {schema}.{table}")
            try:
                with client.execute_query(f"SELECT * FROM {schema}.{table} LIMIT 10") as result:
                    df = result.df()
                    print(df)
            except Exception as e:
                print(f"⚠️ Failed to query {schema}.{table}: {e}")

# Course question: Who has id=17202864 in the stargazers table? Use sql_client.
# Answer: their login is rudolfix
def get_question_stargazer(pipeline):
    with pipeline.sql_client() as client:
        with client.execute_query(f"SELECT distinct id, login FROM github_stargazers where id::VARCHAR = '17202864'") as result:
            stargazers_select = result.df()

            print("\nWho has id=17202864 in the stargazers table?")
            print(stargazers_select)

if __name__ == "__main__":
    run_source()
    select_all_tables(secrets_pipeline)
    get_question_stargazer(secrets_pipeline)