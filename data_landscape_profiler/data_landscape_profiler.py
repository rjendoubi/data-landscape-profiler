import csv
import re
import subprocess

from datetime import datetime
from typing import Dict, List, Optional, Tuple

from impala.error import HiveServer2Error

NOT_FOUND = "NOT FOUND"


class TableTypeError(ValueError):
    pass


class DataLandscapeProfiler:
    def __init__(self, db_cursor) -> None:
        self.db_cursor = db_cursor
        pass

    GET_DATABASES_QUERY = "show databases"
    def get_databases(self, db_filter: Optional[str]=None) -> List[str]:
        db_cursor = self.db_cursor
        db_cursor.execute(self.GET_DATABASES_QUERY)

        databases = []
        for row in db_cursor:
            if row[0] in ('sys', 'information_schema'):
                continue
            databases.append(row[0])

        if db_filter:
            pat = re.compile(db_filter)
            databases = list(filter(lambda x: pat.match(x), databases))

        return databases

    GET_TABLES_QUERY = 'SHOW TABLES IN `{}`'
    def get_tables(self, databases: List[str]) -> List[str]:
        tables = []
        db_cursor = self.db_cursor

        for db in databases:
            db_cursor.execute(self.GET_TABLES_QUERY.format(db))
            for row in db_cursor:
                tables.append(f"`{db}`.`{row[0]}`")

        return tables

    DESCRIBE_QUERY = "DESCRIBE FORMATTED {}"

    def get_describe_for_table(self, table: str) -> List[Tuple[str]]:
        """
        Get `DESCRIBE FORMATTED` info for table.

        :param str table: The fully-qualified table name (i.e. including
            database prefix)
        :return: A List of Tuples, where each Tuple is one line of output
            from `DESCRIBE FORMATTED`.
        :raises ValueError: if the Location is not found
        """
        db_cursor = self.db_cursor

        lines = []
        db_cursor.execute(self.DESCRIBE_QUERY.format(table))
        for row in db_cursor:
            lines.append(row)

        return lines

    def get_location_for_table(
        self, table: str, describe_formatted_output: List[Tuple[str]]
    ) -> str:
        """
        Use `DESCRIBE FORMATTED` to get location of table.

        This is not 100% reliable, as table partitions and files can be
        located anywhere; they need not all be under the same directory.
        Therefore, this approach will likely change later.

        TODO: levels of accuracy for table size, current being lowest

        :param str table: The fully-qualified table name (i.e. including
            database prefix)
        :param List[Tuple[str]] describe_formatted_output: raw output of a
            previously run `DESCRIBE FORMATTED` command.
        :return: The location of the table
        :raises ValueError: if the Location is not found
        """
        for row in describe_formatted_output:
            if row[0].startswith('Location:'):
                return row[1]
            if row[0].startswith('Table Type:') and row[1].startswith('VIRTUAL_VIEW'):
                raise(TableTypeError(f"{table} is a VIRTUAL_VIEW"))

        raise ValueError(f"Location not found; got lines:\n{describe_formatted_output}")

    def get_size_for_location(self, location: str) -> int:
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

        raise ValueError(f"Data size not found for location '{location}'; got lines\n{lines}")

    def run(self, run_stamp="", db_filter: Optional[str]=None, max_errors=0) -> Dict:
        table_locs = {}

        if run_stamp == "":
            run_stamp = datetime.now().strftime('%Y%m%d-%H%M%S')

        dbs = self.get_databases(db_filter=db_filter)
        print(f"Got {len(dbs)} databases")

        tables = self.get_tables(dbs)
        print(f"Got {len(tables)} tables")

        with open(f"{run_stamp}.csv", 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["FQTN", "DESCRIBE", "Location", "Size"])

            error_count = 0
            total = len(tables)
            current = 0

            for t in tables:
                current += 1
                try:
                    desc = self.get_describe_for_table(t)
                    loc = self.get_location_for_table(t, desc)
                    # Let this go so that we can do test runs against Impala
                    # from outside a cluster, without hdfs/Kerberos access.
                    try:
                        size = self.get_size_for_location(loc)
                    except FileNotFoundError:
                        size = "ERR (hdfs executable not available)"

                    table_locs[t] = {
                        "location": loc,
                        "size": size}
                    print(f"OK [{current}/{total}]: {table_locs[t]}")
                    writer.writerow(
                        [
                            t,
                            "\n".join(
                                [
                                    "\t".join([x if x else "" for x in row])
                                    for row in desc
                                ]
                            ),
                            loc,
                            size,
                        ]
                    )

                except HiveServer2Error as ex:
                    # Get last in chain
                    while ex.__cause__:
                        ex = ex.__cause__
                    table_locs[t] = {
                        "location": f"{ex}",
                        "size": "N/A"}
                    print(f"ERR [{current}/{total}]: {table_locs[t]}")
                    writer.writerow([t, f"{ex}", "N/A"])
                except TableTypeError as ex:
                    print(f"OK [{current}/{total}]: {ex}")
                except Exception as ex:
                    print(f"{type(ex)} for table {t} [{current}/{total}]:"
                        f" {ex} (line {ex.__traceback__.tb_lineno})")

                    error_count += 1
                    if max_errors > 0 and error_count >= max_errors:
                        print(f"Encountered {max_errors} errors, quitting...")
                        raise(Exception("Too many errors"))

        return table_locs