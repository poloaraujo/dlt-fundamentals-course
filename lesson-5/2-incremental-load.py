import dlt

# We added `created_at` field to the data
data = [
    {
        "id": "1",
        "name": "bulbasaur",
        "size": {"weight": 6.9, "height": 0.7},
        "created_at": "2024-12-01"    # <------- new field
    },
    {
        "id": "4",
        "name": "charmander",
        "size": {"weight": 8.5, "height": 0.6},
        "created_at": "2024-09-01"    # <------- new field
    },
    {
        "id": "25",
        "name": "pikachu",
        "size": {"weight": 6, "height": 0.4},
        "created_at": "2023-06-01"    # <------- new field
    },
]

# Using dlt, we set up an incremental filter to only fetch Pokémon caught after a certain date:

@dlt.source
def pokemon_source():
    # Append
    # If we run the same pipeline again. The pipeline will detect that there are no new records
    # based on the created_at field and the incremental cursor.
    # As a result, no new data will be loaded into the destination.
    @dlt.resource(name='pokemon_appended_incremental', write_disposition='append')
    def pokemon_append(cursor_date=dlt.sources.incremental("created_at", initial_value="2024-01-01")):
        yield data

    return pokemon_append

def run_source():
    pipeline = dlt.pipeline(
        pipeline_name="poke_pipeline_incremental",
        destination="duckdb",
        dataset_name="pokemon_data",
    )

    load_info = pipeline.run(pokemon_source())
    print(load_info)

    # explore loaded data
    select_all_tables(pipeline)

# This is a method that I wrote to go through all of the tables loaded by the pipeline. It is not part of the course
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
                print("Tables found:", table_names)
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
