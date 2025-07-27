## Lesson 1

**Using the code from the previous cell, fetch the data from the pokemon table into a dataframe and count the number of columns in the table pokemon.**

6 columns and 3 rows. See dlt-lesson-1.py, line 34-36

```
SQL Client select
   id        name  size__weight  size__height       _dlt_load_id         _dlt_id
0   1   bulbasaur           6.9           0.7  1753576239.209686  iJhcg7BEE2XsWA
1   4  charmander           8.5           0.6  1753576239.209686  4D39MFJ6rFN0pg
2  25     pikachu           6.0           0.4  1753576239.209686  dDnrAy3+Tg0jiQ
```

## Lesson 2

**How many columns has the github_repos table? Use duckdb connection, sql_client or pipeline.dataset().**

106 columns<br>
[30 rows x 106 columns]. Run dlt-lesson-2-exercise.py to see the results.

**How many columns has the github_stargazer table? Use duckdb connection, sql_client or pipeline.dataset().**

23 columns<br>
[158 rows x 23 columns]. Run dlt-lesson-2-exercise.py to see the results.

## Lesson 3

**What type of pagination should we use for the GitHub API?**

You should use the HeaderLinkPaginator, which dly provides out-of-the-box for APIs like GitHub<br>
GitHub uses HTTP Link Header-based pagination — this is commonly referred to as "link header" pagination.

```
https://api.github.com/repos/dlt-hub/dlt/stargazers
https://api.github.com/repos/dlt-hub/dlt/stargazers?page=2
```

**Course question: Who has id=17202864 in the stargazers table? Use sql_client.**

Their login is _rudolfix_<br>
See 2-secrets-pipeline.py, lines 83-91

```
         id     login
0  17202864  rudolfix
```

## Lesson 4

**How many columns has the issues table?**
136<br>
Run `rest_api_pipeline.py`

**How many columns has the contributors table?**
22<br>
Run `rest_api_pipeline.py`

**How many columns does the family table have?**
37<br>
Run `mysql_pipeline.py`

**How many columns does the userdata table have?**
15<br>
Run `parquet_pipeline.py`

## Lesson 5

**How many columns does the comments table have?**
58<br>
Run `5-github-pipeline-exercise.py`

