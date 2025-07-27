import dlt

# Sample data containing pokemon details
data = [
    {"id": "1", "name": "bulbasaur", "size": {"weight": 6.9, "height": 0.7}},
    {"id": "4", "name": "charmander", "size": {"weight": 8.5, "height": 0.6}},
    {"id": "25", "name": "pikachu", "size": {"weight": 6, "height": 0.4}},
]

@dlt.source
def pokemon_source():
    # Append
    @dlt.resource(name='pokemon_appended', write_disposition='append')
    def pokemon_append():
        yield data

    # Replace
    @dlt.resource(name='pokemon_replaced', write_disposition='replace')
    def pokemon_replace():
        yield data

    # Merge
    @dlt.resource(name='pokemon_merged', write_disposition='merge', primary_key='id')
    def pokemon_merge():
        yield data

    return pokemon_append, pokemon_replace, pokemon_merge

def run_source():
    pipeline = dlt.pipeline(
        pipeline_name="poke_pipeline",
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
