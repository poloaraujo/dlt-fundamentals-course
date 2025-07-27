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
        name="repos",
        write_disposition="merge",
        primary_key="id"
    )
    def github_repos(cursor_date=dlt.sources.incremental("updated_at", initial_value="2024-12-01")):
        params = {
            "since": cursor_date.last_value,  # <--- use last_value to request only new data from API
            "status": "open"
        }
        for page in client.paginate("orgs/dlt-hub/repos", params=params):
            yield page

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
            yield from page

    @dlt.transformer(
        data_from=github_issues,
        name="issue_comments",
        write_disposition="merge",
        primary_key="id"
    )
    def github_issue_comments(issue):
        issue_number = issue["number"]
        for page in client.paginate(f"repos/dlt-hub/dlt/issues/{issue_number}/comments"):
            for comment in page:
                comment["issue_number"] = issue_number
                comment["issue_id"] = issue["id"]
                yield comment

    # The Contributors endpoint doesn't have a field we can use as a cursor,
    # so we'll use merge without incremental
    @dlt.resource(
        name="contributors",
        write_disposition="merge",
        primary_key="id"
    )
    def github_contributors():
        for page in client.paginate("repos/dlt-hub/dlt/contributors"):
            yield page

    @dlt.resource(
        name="comments",
        write_disposition="merge",
        primary_key="id"
    )
    def github_comments(): # Comments has 6 columns
        for page in client.paginate("repos/dlt-hub/dlt/pulls/comments"):
            yield page

    return github_repos, github_issues, github_contributors, github_issue_comments, github_comments

def run_source():
# define new dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="github_incremental_exercise",
        destination="duckdb"
    )

    load_info = pipeline.run(github_source())
    print(load_info)

    # explore loaded data
    select_all_tables(pipeline)


def select_all_tables(pipeline_name):
    # This will connect to the existing pipeline configuration and .duckdb file by name.
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination='duckdb')

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

# How many columns does the comments table have??
# 58 columns
def select_comments(pipeline_name):
    # This will connect to the existing pipeline configuration and .duckdb file by name.
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination='duckdb')

    # Query comments
    with pipeline.sql_client() as client:
        with client.execute_query("select * from comments") as result:
            comments = result.df()
            print("\ncomments")
            print(comments)


if __name__ == "__main__":
    run_source()
    select_all_tables("github_incremental_exercise")
    select_comments("github_incremental_exercise")