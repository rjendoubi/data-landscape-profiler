import os
import subprocess

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
db_cursor.execute(GET_DATABASES_QUERY)

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

def get_size_for_location(location: str) -> int:
  """
  Use `hdfs` subprocess to get size in bytes of given location.
  
  :param str location: The location to pass to `hdfs`; can use `s3a://` scheme.
  :return: The size in bytes
  :raises ValueError: if the location is not found in the hdfs stdout
  :raises ValueError: if the output line does not begin with size in bytes as expected
  """
  completed_process = subprocess.run(
    ["hdfs", "dfs", "-du", "-s", location], capture_output=True, text=True)
  
  # Check truthiness to remove empty lines
  lines = list(filter(lambda x: x, completed_process.stdout.split("\n")))
  
  for line in lines:
    if line.endswith(location):
      return int(line.split(" ")[0])

  raise ValueError("Location size not found")
