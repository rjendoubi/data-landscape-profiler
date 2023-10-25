import subprocess

from typing import Dict, List


NOT_FOUND = "NOT FOUND"

class DataLandscapeProfiler:
    def __init__(self, db_cursor) -> None:
        self.db_cursor = db_cursor
        pass

    GET_DATABASES_QUERY = "show databases"
    def get_databases(self) -> List[str]:
        db_cursor = self.db_cursor
        db_cursor.execute(self.GET_DATABASES_QUERY)

        databases = []
        for row in db_cursor:
            if row[0] in ('sys', 'information_schema'):
                continue
            databases.append(row[0])

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

    DESCRIBE_QUERY = 'DESCRIBE FORMATTED {}'
    def get_location_for_table(self, table: str) -> str:
        """
        Use `DESCRIBE FORMATTED` to get location of table.

        This is not 100% reliable, as table partitions and files can be
        located anywhere; they need not all be under the same directory.
        Therefore, this approach will likely change later.

        TODO: levels of accuracy for table size, current being lowest

        :param str table: The fully-qualified table name (i.e. including
                          database prefix)
        :return: The location of the table
        :raises ValueError: if the Location is not found
        """
        db_cursor = self.db_cursor

        db_cursor.execute(self.DESCRIBE_QUERY.format(t))
        for row in db_cursor:
            print(row)
            if row[0].startswith('Location:'):
                return row[1]
            raise ValueError("Location not found")

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

    def run(self) -> Dict:
        table_locs = {}
        dbs = self.get_databases()
        tables = self.get_tables(dbs)

        print(f"Got {len(tables)} tables")
        for t in tables:
            try:
                loc = self.get_location_for_table(t)
                size = self.get_size_for_location(loc)
                table_locs[t] = {
                    "location": loc,
                    "size": size}
            except Exception as ex:
                print(f"{type(ex)} for table {t}:"
                     f" {ex} (line {ex.__traceback__.tb_lineno})")

        return table_locs