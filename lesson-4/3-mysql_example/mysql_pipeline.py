import dlt
from dlt.sources.sql_database import sql_database

import duckdb

mysql_source = sql_database(
    "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
    table_names=["family",]
)

def run_source():
    pipeline = dlt.pipeline(
        pipeline_name="sql_database_pipeline",
        destination="duckdb",
        dataset_name="sql_data",
        dev_mode=True,
    )

    load_info = pipeline.run(mysql_source)
    print(load_info)

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

# How many columns does the family table have??
# 37 columns
def select_family(pipeline_name):
    # This will connect to the existing pipeline configuration and .duckdb file by name.
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination='duckdb')

    # Query family
    with pipeline.sql_client() as client:
        with client.execute_query("select * from family") as result:
            Family = result.df()
            print("\nFamily")
            print(Family)

if __name__ == "__main__":
    run_source()
    select_all_tables("sql_database_pipeline")
    select_family("sql_database_pipeline")
