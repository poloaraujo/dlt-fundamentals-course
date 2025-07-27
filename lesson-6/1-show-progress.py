import dlt

pipeline = dlt.pipeline(
    pipeline_name="progress_pipeline",
    destination="duckdb",
    progress="log"
)


# We load the data into the table_name table
def run_source():
    load_info = pipeline.run(
        [
            {"id": 1},
            {"id": 2},
            {"id": 3, "nested": [{"id": 1}, {"id": 2}]},
        ],
        table_name="items",
    )
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

# How many columns does the userdata table have??
# 15 columns
def select_userdata(pipeline_name):
    # This will connect to the existing pipeline configuration and .duckdb file by name.
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination='duckdb')

    # Query userdata
    with pipeline.sql_client() as client:
        with client.execute_query("select * from userdata") as result:
            userdata = result.df()
            print("\nuserdata")
            print(userdata)

if __name__ == "__main__":
    run_source()
    select_all_tables("progress_pipeline")