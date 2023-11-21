import csv
import os

from datetime import datetime
from pprint import pprint

import cml.data_v1 as cmldata
from data_landscape_profiler import DataLandscapeProfiler

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

dlp = DataLandscapeProfiler(db_cursor)
table_locs = dlp.run()
pprint(table_locs, sort_dicts=True)

dt_str = datetime.now().strftime('%Y%m%d-%H%M%S')
with open(f"{dt_str}.csv", 'w') as csvfile:
    writer = csv.writer(csvfile)
    for key in sorted(table_locs):
        writer.writerow([key, table_locs[key]["location"],table_locs[key]["size"]])