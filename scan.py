import os

from pprint import pprint

from data_landscape_profiler import DataLandscapeProfiler

IMPALA_USERNAME = os.getenv("HADOOP_USER_NAME")
if not IMPALA_USERNAME:
    IMPALA_USERNAME = os.getenv("DLP_USERNAME")
IMPALA_PASSWORD = os.getenv("WORKLOAD_PASSWORD")
if not IMPALA_PASSWORD:
    IMPALA_PASSWORD = os.getenv("DLP_PASSWORD")
if not IMPALA_USERNAME or not IMPALA_PASSWORD:
    raise ValueError("Impala username and password required")

CONNECTION_NAME = os.getenv("DLP_CONNECTION_NAME")
HOSTNAME = os.getenv("DLP_HOSTNAME")
if not CONNECTION_NAME and not HOSTNAME:
    raise ValueError("Either CONNECTION_NAME or HOSTNAME required")

# Get Impala db cursor
db_cursor = None

try:
    import cml.data_v1 as cmldata

    conn = cmldata.get_connection(
        CONNECTION_NAME, {"USERNAME": IMPALA_USERNAME, "PASSWORD": IMPALA_PASSWORD}
    )

    db_cursor = conn.get_cursor()

except ImportError:
    from impala.dbapi import connect

    # Standard args which work for CDW
    conn = connect(
        host = HOSTNAME,
        port = 443,
        use_ssl=True,
        user=IMPALA_USERNAME,
        password=IMPALA_PASSWORD,
        use_http_transport=True,
        http_path='cliservice',
        auth_mechanism='PLAIN'
        )

    db_cursor = conn.cursor()

dlp = DataLandscapeProfiler(db_cursor)
table_locs = dlp.run()
pprint(table_locs, sort_dicts=True)