import dlt

# Sample pokemon data
pokemon_data = [
    {"id": "1", "name": "bulbasaur", "size": {"weight": 6.9, "height": 0.7}},
    {"id": "4", "name": "charmander", "size": {"weight": 8.5, "height": 0.6}},
    {"id": "25", "name": "pikachu", "size": {"weight": 6, "height": 0.4}},
]

# Set pipeline
# dev_mode: If you set this to True, dlt will add a timestamp to your dataset name every time you
# create a pipeline. This means a new dataset will be created each time you create a pipeline.
# But you also get some weird schema names, so beware
pipeline = dlt.pipeline(
    pipeline_name="quick_start",
    destination="duckdb",
    dataset_name="pokemon_schema",
    #dev_mode=True,
)

# Run data
# I've set write_disposition="replace" to drop existing data and replace it.
# The default behavior is "append", which duplicates records
load_info = pipeline.run(pokemon_data, table_name="pokemon", write_disposition="replace")
print(load_info)

# Query data from 'pokemon' using the SQL client
# This is one way to query the data directly from the pipeline
with pipeline.sql_client() as client:
    with client.execute_query(f"select * from pokemon") as cursor:
        sql_client_select = cursor.df()

# Display the data
print("\n")
print("SQL Client select")
print(sql_client_select)

# Here's an example of how to retrieve data from a pipeline and load it into a Pandas DataFrame or a PyArrow Table.
# The pipeline.dataset function gets the dataset
print("\n")
print("Select data from a pipeline and load it into a Pandas DataFrame")
dataset = pipeline.dataset(dataset_type="default")
print(dataset.pokemon.df())

# Count records - 3
with pipeline.sql_client() as client:
    with client.execute_query(f"select count(*) from pokemon") as cursor:
        sql_client_count = cursor.df()

print("\n")
print("SQL Client count(*)")
print(sql_client_count)

print("\n")
print("Count data from a pipeline and load it into a Pandas DataFrame")
dataset = pipeline.dataset(dataset_type="default")
print(dataset.row_counts().df())