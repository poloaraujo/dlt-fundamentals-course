import duckdb
import streamlit as st # Use to visualize data

# A database '<pipeline_name>.duckdb' was created in working directory so just connect to it
pipeline_duckdb = 'quick_start.duckdb'

# Define the dataset (schema) that you'll connect to
dataset = 'pokemon_schema'

# Connect to the DuckDB database
conn = duckdb.connect(f'{pipeline_duckdb}')

# Show all schemas
print(conn.sql("SELECT DISTINCT schema_name FROM information_schema.schemata").df())

# Set search path to the dataset
conn.sql(f"SET search_path = '{dataset}'")

# Describe the dataset
describe_dataset = conn.sql("DESCRIBE").df()

# Fetch all data from 'pokemon' as a DataFrame
table_name = 'pokemon'
select_table = conn.sql(f"select * from {table_name}")

# Count records
count_table = conn.sql(f"select count(*) as record_count from {table_name}")

# Streamlit UI
st.title("Describe dataset")
st.write(f"{dataset} schema")
st.dataframe(describe_dataset)

st.title("Select table")
st.write(f"{dataset}.{table_name}")
st.dataframe(select_table)

st.title("Count records")
st.write(f"{dataset}.{table_name}")
st.dataframe(count_table)
