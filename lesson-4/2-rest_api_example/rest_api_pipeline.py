# This is the result of running dlt init rest_api duckdb

from typing import Any, Optional

import dlt
from dlt.common.pendulum import pendulum
from dlt.sources.rest_api import (
    RESTAPIConfig,
    check_connection,
    rest_api_resources,
    rest_api_source,
)

import duckdb


@dlt.source(name="github")
def github_source(access_token: Optional[str] = dlt.secrets.value) -> Any:
    # Create a REST API configuration for the GitHub API
    # Use RESTAPIConfig to get autocompletion and type checking
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.github.com",
            "auth": {
                "token": dlt.secrets["sources.access_token"], # <--- we already configured access_token above
            },
            "paginator": "header_link" # <---- set up paginator type
        },
        "resources": [  # <--- list resources
            {
                "name": "issues",
                "endpoint": {
                    "path": "repos/dlt-hub/dlt/issues",
                    "params": {
                        "state": "open",
                    },
                },
            },
            {
                "name": "issue_comments", # <-- here we declare dlt.transformer
                "endpoint": {
                    "path": "repos/dlt-hub/dlt/issues/{issue_number}/comments",
                    "params": {
                        "issue_number": {
                            "type": "resolve", # <--- use type 'resolve' to resolve {issue_number} for transformer
                            "resource": "issues",
                            "field": "number",
                        },

                    },
                },
            },
            {
            "name": "contributors",
            "endpoint": {
                "path": "repos/dlt-hub/dlt/contributors",
            },
            },
        ],
    }

    yield from rest_api_resources(config)


def load_github() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_github",
        destination='duckdb',
        dataset_name="rest_api_data",
    )

    load_info = pipeline.run(github_source())
    print(load_info)  # noqa: T201


def load_pokemon() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_pokemon",
        destination='duckdb',
        dataset_name="rest_api_data",
    )

    pokemon_source = rest_api_source(
        {
            "client": {
                "base_url": "https://pokeapi.co/api/v2/",
                # If you leave out the paginator, it will be inferred from the API:
                # "paginator": "json_link",
            },
            "resource_defaults": {
                "endpoint": {
                    "params": {
                        "limit": 1000,
                    },
                },
            },
            "resources": [
                "pokemon",
                "berry",
                "location",
            ],
        }
    )

    def check_network_and_authentication() -> None:
        (can_connect, error_msg) = check_connection(
            pokemon_source,
            "not_existing_endpoint",
        )
        if not can_connect:
            pass  # do something with the error message

    check_network_and_authentication()

    load_info = pipeline.run(pokemon_source)
    print(load_info)  # noqa: T201

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

# How many columns has the issues table?
# 136 columns
def select_issues(pipeline_name):
    # This will connect to the existing pipeline configuration and .duckdb file by name.
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination='duckdb')

    # Query Issues
    with pipeline.sql_client() as client:
        with client.execute_query("select * from issues") as result:
            issues = result.df()
            print("\nIssues")
            print(issues)

# How many columns has the contributors table?
# 22 columns
def select_contributors(pipeline_name):
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination='duckdb')
    with pipeline.sql_client() as client:
        with client.execute_query("select * from contributors") as result:
            contributors = result.df()
            print("\nContributors")
            print(contributors)


if __name__ == "__main__":
    #load_github()
    #load_pokemon()
    #select_all_tables("rest_api_github")
    select_issues("rest_api_github")
    select_contributors("rest_api_github")
