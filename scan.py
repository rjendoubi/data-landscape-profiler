import os

import cml.data_v1 as cmldata

from pprint import pprint

CONNECTION_NAME = "iceberg-hive"
HMS_USERNAME = os.getenv("HADOOP_USER_NAME")
HMS_PASSWORD = os.getenv("WORKLOAD_PASSWORD")

conn = cmldata.get_connection(
    CONNECTION_NAME, {"USERNAME": HMS_USERNAME, "PASSWORD": HMS_PASSWORD}
)

# Alternate Sample Usage to get DB API Connection interface
db_conn = conn.get_base_connection()

# Alternate Sample Usage to get DB API Cursor interface
db_cursor = conn.get_cursor()


GET_DATABASES_QUERY = "show databases"
db_cursor.execute(EXAMPLE_SQL_QUERY)

databases = []
for row in db_cursor:
  if row[0] in ('sys', 'information_schema'):
    continue
  databases.append(row[0])
  
print(f"Got {len(databases)} databases")
for db in databases:
  print(db)

tables = []

GET_TABLES_QUERY = 'SHOW TABLES IN `{}`'
for db in databases:
  db_cursor.execute(GET_TABLES_QUERY.format(db))
  for row in db_cursor:
    tables.append(f"`{db}`.`{row[0]}`")


DESCRIBE_QUERY = 'DESCRIBE FORMATTED {}'
NOT_FOUND = "NOT FOUND"
table_locs = {}
print(f"Got {len(tables)} tables")
for t in tables:
  print(t)
  db_cursor.execute(DESCRIBE_QUERY.format(t))
  for row in db_cursor:
    print(row)
    if row[0].startswith('Location:'):
      table_locs[t] = {"location": row[1]}
      break
    table_locs[t] = {"location": NOT_FOUND}

pprint(table_locs, sort_dicts=True)