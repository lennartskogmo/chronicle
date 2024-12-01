# Base reader.
class BaseReader:

    # Read from table using single query.
    def _read_single(self, table):
        return spark.read.jdbc(properties=self.properties, url=self.url, table=table, numPartitions=1)

    # Read from table using multiple parallel queries.
    def _read_parallel(self, table, parallel_column, parallel_number, lower_bound, upper_bound):
        return spark.read.jdbc(
            properties    = self.properties,
            url           = self.url,
            table         = table,
            numPartitions = parallel_number,
            column        = parallel_column,
            lowerBound    = lower_bound,
            upperBound    = upper_bound
        )

    # Execute query and return data frame.
    def query(self, query):
        return self._read_single(f"({query}) AS q")

    # Read from table and return data frame containing only key columns.
    def read_key(self, table, key, where=None):
        if isinstance(key, str):
            df = self._read_single(table).select(key)
        elif isinstance(key, list):
            df = self._read_single(table).select(*key)
        else:
            raise Exception("Invalid key")
        return filter(df=df, where=where)

    # Read from table and return data frame.
    def read(self, table, exclude=None, where=None, parallel_column=None, parallel_number=None):
        if parallel_column is None and parallel_number is None:
            df = self._read_single(table)
        elif parallel_column is not None and parallel_number is not None:
            bounds = self.get_bounds(table=table, parallel_column=parallel_column, where=where)
            df = self._read_parallel(
                table           = table,
                parallel_column = parallel_column,
                parallel_number = parallel_number,
                lower_bound     = bounds["l"],
                upper_bound     = bounds["u"]
            )
        else:
            raise Exception("Invalid parallel_column and parallel_number combination")
        return filter(df=df, exclude=exclude, where=where)

    # Read from table and return data frame containing rows where the column value is equal to the provided value.
    def read_equal_to(self, table, column, value, exclude=None, where=None, parallel_column=None, parallel_number=None):
        if parallel_column is None and parallel_number is None:
            df = self._read_single(table)
        elif parallel_column is not None and parallel_number is not None:
            bounds = self.get_bounds_equal_to(table=table, column=column, value=value, parallel_column=parallel_column, where=where)
            df = self._read_parallel(
                table           = table,
                parallel_column = parallel_column,
                parallel_number = parallel_number,
                lower_bound     = bounds["l"],
                upper_bound     = bounds["u"]
            )
        else:
            raise Exception("Invalid parallel_column and parallel_number combination")
        return filter(df=df.where(col(column) == value), exclude=exclude, where=where)

    # Read from table and return data frame containing rows where the column value is greater than the provided value.
    def read_greater_than(self, table, column, value=None, exclude=None, where=None, parallel_column=None, parallel_number=None):
        # Fall back to normal read if value is None so incremental loads can be initalized.
        if value is None:
            return self.read(table=table, where=where, parallel_column=parallel_column, parallel_number=parallel_number)
        else:
            if parallel_column is None and parallel_number is None:
                df = self._read_single(table)
            elif parallel_column is not None and parallel_number is not None:
                bounds = self.get_bounds_greater_than(table=table, column=column, value=value, parallel_column=parallel_column, where=where)
                df = self._read_parallel(
                    table           = table,
                    parallel_column = parallel_column,
                    parallel_number = parallel_number,
                    lower_bound     = bounds["l"],
                    upper_bound     = bounds["u"]
                )
            else:
                raise Exception("Invalid parallel_column and parallel_number combination")
            return filter(df=df.where(col(column) > value), exclude=exclude, where=where)

    # Get bounds for parallel read.
    def get_bounds(self, table, parallel_column, where=None):
        df = self.query(f"SELECT MIN({parallel_column}) AS l, MAX({parallel_column}) AS u FROM {table} WHERE ({self.substitute(where)})")
        return df.toPandas().to_dict(orient="records")[0]

    # Get bounds for parallel equal to read.
    def get_bounds_equal_to(self, table, column, value, parallel_column, where=None):
        df = self.query(f"SELECT MIN({parallel_column}) AS l, MAX({parallel_column}) AS u FROM {table} WHERE ({self.substitute(where)}) AND ({column} = {self.quote(value)})")
        return df.toPandas().to_dict(orient="records")[0]

    # Get bounds for parallel greater than read.
    def get_bounds_greater_than(self, table, column, value, parallel_column, where=None):
        df = self.query(f"SELECT MIN({parallel_column}) AS l, MAX({parallel_column}) AS u FROM {table} WHERE ({self.substitute(where)}) AND ({column} > {self.quote(value)})")
        return df.toPandas().to_dict(orient="records")[0]

    # Return list of distinct values from the specified table and column, optionally filtered by greater_than and where.
    def get_distinct(self, table, column, greater_than=None, where=None):
        if greater_than is None:
            query = f"SELECT DISTINCT {column} AS d FROM {table} WHERE ({self.substitute(where)}) ORDER BY {column}"
        else:
            query = f"SELECT DISTINCT {column} AS d FROM {table} WHERE ({self.substitute(where)}) AND ({column} > {self.quote(greater_than)}) ORDER BY {column}"
        if self.__class__.__name__ == "SqlserverReader":
            query += " OFFSET 0 ROWS"
        return list(self.query(query).toPandas()['d'])

    # Substitute where with 1=1 if where is not present.
    def substitute(self, where):
        if where is None:
            where = "1=1"
        return where

    # Quote value so it can be used as part of sql statement.
    def quote(self, value):
        if value.__class__.__name__ in ["date", "datetime", "str", "Timestamp"]:
            value = f"'{value}'"
        return value
