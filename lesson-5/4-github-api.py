import dlt
from dlt.sources.helpers import requests
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

source_access_token = dlt.secrets["sources.access_token"]

@dlt.source
def github_source(access_token=source_access_token):
    client = RESTClient(
            base_url="https://api.github.com",
            auth=BearerTokenAuth(token=access_token),
            paginator=HeaderLinkPaginator(),
    )

    @dlt.resource(
        name="issues",
        write_disposition="merge",
        primary_key="id"
    )
    def github_issues(cursor_date=dlt.sources.incremental("updated_at", initial_value="2024-12-01")):
        params = {
            "since": cursor_date.last_value,  # <--- use last_value to request only new data from API
            "status": "open"
        }
        for page in client.paginate("repos/dlt-hub/dlt/issues", params=params):
            yield page


    return github_issues


def run_source():
# define new dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="github_incremental",
        destination="duckdb"
    )

    load_info = pipeline.run(github_source())
    print(load_info)

    # explore loaded data
    select_all_tables(pipeline)


def select_all_tables(pipeline):
    # List all table names from the database
    with pipeline.sql_client() as client:
        table_names = []

        with client.execute_query("SELECT table_name FROM information_schema.tables") as table:
            print(f"\nAll tables loaded in {pipeline.pipeline_name}")

            tables_df = table.df()
            print(tables_df)

            table_names = []
            if tables_df is not None and not tables_df.empty:
                for _, row in tables_df.iterrows():
                    table_names.append(row["table_name"])
                print("\nTables found:", table_names)
            else:
                print("⚠️ No tables found or query failed.")

    # Loop through each table name and fetch the data
    for table in table_names:
        print(f"\n{pipeline.pipeline_name} - {table}")

        # Dynamically access the table
        dataset = getattr(pipeline.dataset(dataset_type="default"), table).df()
        print(dataset)

if __name__ == "__main__":
    run_source()