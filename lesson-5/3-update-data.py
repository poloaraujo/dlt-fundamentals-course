import dlt

# We added `created_at` field to the data
data = [
    {
        "id": "1",
        "name": "bulbasaur",
        "size": {"weight": 6.9, "height": 0.7},
        "created_at": "2024-12-01",
        "updated_at": "2024-12-01"    # <------- new field
    },
    {
        "id": "4",
        "name": "charmander",
        "size": {"weight": 8.5, "height": 0.6},
        "created_at": "2024-09-01",
        "updated_at": "2024-09-01"    # <------- new field
    },

    # Toggle between the Pikachu dicts to see merge in action
    {
        "id": "25",
        "name": "pikachu",
        "size": {"weight": 9, "height": 0.4}, # <----- pikachu gained weight from 6 to 9
        "created_at": "2023-06-01",
        "updated_at": "2024-12-16"    # <------- new field, information about pikachu has updated
    },
    # {
    #     "id": "25",
    #     "name": "pikachu",
    #     "size": {"weight": 7.5, "height": 0.4}, # <--- pikachu lost weight
    #     "created_at": "2023-06-01",
    #     "updated_at": "2024-12-23"  # <--- data about his weight was updated a week later
    # },
]

# Using dlt, we set up an incremental filter to only fetch Pokémon caught after a certain date:

@dlt.source
def pokemon_source():
    # The incremental cursor keeps an eye on the updated_at field.
    # Every time the pipeline runs, it only processes records with updated_at
    # values greater than the last run.
    @dlt.resource(
        name="pokemon",
        write_disposition="merge",  # <--- change write disposition from 'append' to 'merge'
        primary_key="id",  # <--- set a primary key
    )
    def pokemon_merge(cursor_date=dlt.sources.incremental("updated_at", initial_value="2024-01-01")):  # <--- change the cursor name from 'created_at' to 'updated_at'
        yield data

    return pokemon_merge

def run_source():
    pipeline = dlt.pipeline(
        pipeline_name="poke_pipeline_merge",
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
