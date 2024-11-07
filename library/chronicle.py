# Databricks notebook source
from os import environ
from re import match, sub
from urllib.parse import urlencode
from pyspark.sql.functions import coalesce, col, concat_ws, current_timestamp, expr, lag, lead, lit, row_number, when, xxhash64
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

# Perform additional initialization if library is imported as module.
if __name__ != "__main__":
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

# Define configuration locations.
CHRONICLE  = "__chronicle"                     # The configuration schema.
CONNECTION = "__chronicle.connection"          # The data connection configuration table.
OBJECT     = "__chronicle.object"              # The data object configuration table.
EXTERNAL   = environ.get("CHRONICLE_EXTERNAL") # The path to external table storage location.

# Define metadata column names.
KEY        = "__key"        # The record primary key.
CHECKSUM   = "__checksum"   # The record checksum.
OPERATION  = "__operation"  # The type of operation that produced the record.
LOADED     = "__loaded"     # The time the record was loaded.
ANONYMIZED = "__anonymized" # Reserved for the time the record was anonymized.


# Return new reader instance if argument is string, else act as pass-through function and return argument itself.
def get_reader(reader):
    if isinstance(reader, str):
        # Lookup connection details.
        connection_with_secrets = spark.table(CONNECTION).filter(f"ConnectionName == '{reader}'").collect()[0].asDict()
        for key, value in connection_with_secrets.items():
            if isinstance(value, str) and value.startswith("<") and value.endswith(">"):
                connection_with_secrets[key] = dbutils.secrets.get(scope="kv", key=value[1:-1])
        # Map connection details to reader constructor arguments.
        reader_arguments = {}
        for key, value in connection_with_secrets.items():
            if value is not None:
                if key == "Host"     : reader_arguments["host"]     = value
                if key == "Port"     : reader_arguments["port"]     = value
                if key == "Database" : reader_arguments["database"] = value
                if key == "Username" : reader_arguments["username"] = value
                if key == "Password" : reader_arguments["password"] = value
        # Instantiate reader.
        reader = globals()[connection_with_secrets["Reader"]]
        reader = reader(**reader_arguments)
    return reader

# Validate mode against dynamic list of values.
def validate_mode(valid, mode):
    if mode not in valid:
        raise Exception("Invalid mode")


# Apply column and row filters if present, else act as pass-through function and return unaltered data frame.
def filter(df, exclude=None, where=None):
    if where is not None:
        df = df.where(where)
    if exclude is not None:
        df = exclude_columns(df, exclude)
    return df

# Exclude columns from data frame.
# Used by reader to remove columns before reading from source.
def exclude_columns(df, exclude):
    if isinstance(exclude, str):
        return df.drop(exclude)
    elif isinstance(exclude, list):
        return df.drop(*exclude)
    else:
        raise Exception("Invalid exclude")

# Drop columns from data frame.
# Used by writer to remove columns before writing to target.
def drop_columns(df, drop):
    if isinstance(drop, str):
        drop = conform_column_name(drop)
        return df.drop(drop)
    elif isinstance(drop, list):
        drop = [conform_column_name(c) for c in drop]
        return df.drop(*drop)
    else:
        raise Exception("Invalid drop")

# Add CHECKSUM column to beginning of data frame.
def add_checksum_column(df, ignore=None):
    if ignore is None:
        columns = sorted(df.columns)
    elif isinstance(ignore, str):
        ignore = conform_column_name(ignore)
        columns = sorted(c for c in df.columns if c != ignore)
    elif isinstance(ignore, list):
        ignore = [conform_column_name(c) for c in ignore]
        columns = sorted(c for c in df.columns if c not in ignore)
    else:
        raise Exception("Invalid ignore")
    return df.select([xxhash64(concat_ws("<|>", *[coalesce(col(c).cast(StringType()), lit('')) for c in columns])).alias(CHECKSUM), "*"])

# Add KEY column to beginning of data frame.
def add_key_column(df, key):
    if isinstance(key, str):
        return df.select([df[conform_column_name(key)].alias(KEY), "*"])
    elif isinstance(key, list):
        key = sorted([conform_column_name(c) for c in key])
        return df.select([concat_ws("-", *key).alias(KEY), "*"])
    else:
        raise Exception("Invalid key")

# Add hash columns to end of data frame.
def add_hash_columns(df, hash):
    if isinstance(hash, str):
        hash = conform_column_name(hash)
        df = df.withColumn(hash+"_hash", md5(col(hash).cast(StringType())))
    elif isinstance(hash, list):
        hashes = [conform_column_name(c) for c in hash]
        for hash in hashes:
            df = df.withColumn(hash+"_hash", md5(col(hash).cast(StringType())))
    else:
        raise Exception("Invalid hash")
    return df

# Conform data frame column names to naming convention and check for duplicate column names.
def conform_column_names(df):
    df = df.toDF(*[conform_column_name(c) for c in df.columns])
    duplicates = {c for c in df.columns if df.columns.count(c) > 1}
    if duplicates:
        raise Exception(f"Duplicate column name(s): {duplicates}")
    return df

# Conform column name to naming convention.
def conform_column_name(cn):
    cn = cn.lower()                                # Convert to lowercase.
    cn = cn.translate(str.maketrans("æøå", "eoa")) # Replace Norwegian characters with Latin characters.
    cn = sub("[^a-z0-9]+", "_", cn)                # Replace all characters except letters and numbers with underscores.
    cn = sub("^_|_$", "", cn)                      # Remove leading and trailing underscores.
    return cn


# Return the current maximum value from the specified Delta table and column, optionally adjusted by offset.
def get_max(table, column, offset=None):
    # Return early if table does not exist.
    if not spark.catalog._jcatalog.tableExists(table):
        return None
    # Prepare query.
    column = conform_column_name(column)
    if offset is None:
        query = f"SELECT MAX({column}) FROM {table}"
    else:
        if (isinstance(offset, int) and not isinstance(offset, bool)) or (isinstance(offset, str) and match("^[0-9]+$", offset)):
            query = f"SELECT MAX({column}) - {offset} FROM {table}"
        elif isinstance(offset, str):
            query = f"SELECT MAX({column}) - INTERVAL {offset} FROM {table}"
        else:
            raise Exception("Invalid offset")
    # Execute query.
    return spark.sql(query).collect()[0][0]

# Return number of output rows from the last operation performed against the specified Delta table.
def get_num_output_rows(table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()[0][12]["numOutputRows"]

# Read from Delta table and return data frame containing current rows that have not been marked as deleted.
def read_active(table, select=None):
    ws = Window.partitionBy(col(KEY)).orderBy(col(LOADED).desc())
    df = spark.table(table).withColumn("_rn", row_number().over(ws)) \
        .where(col("_rn") == 1).drop(col("_rn")) \
        .where(col(OPERATION) != "D")
    if select is None:
        return df
    elif isinstance(select, list) or isinstance(select, str):
        return df.selectExpr(select)
    else:
        raise Exception("Invalid select")


# Perform full load.
def load_full(
        reader, source, target, key, mode="insert_update_delete",
        exclude=None, ignore=None, hash=None, drop=None, where=None, parallel_number=None, parallel_column=None
    ):
    validate_mode(mode=mode, valid=["insert_update", "insert_update_delete"])
    reader = get_reader(reader=reader)
    writer = DeltaBatchWriter(mode=mode, table=target, key=key, ignore=ignore, hash=hash, drop=drop)
    df = reader.read(table=source, exclude=exclude, where=where, parallel_number=parallel_number, parallel_column=parallel_column)
    return writer.write(df)

# Perform incremental load.
def load_incremental(
        reader, source, target, key, bookmark_column, bookmark_offset=None, mode="insert_update",
        exclude=None, ignore=None, hash=None, drop=None, where=None, parallel_number=None, parallel_column=None
    ):
    validate_mode(mode=mode, valid=["insert_update"])
    reader = get_reader(reader=reader)
    writer = DeltaBatchWriter(mode=mode, table=target, key=key, ignore=ignore, hash=hash, drop=drop)
    max = get_max(table=target, column=bookmark_column, offset=bookmark_offset)
    df = reader.read_greater_than(table=source, column=bookmark_column, value=max, exclude=exclude, where=where, parallel_number=parallel_number, parallel_column=parallel_column)
    return writer.write(df)


# Delta table batch writer.
class DeltaBatchWriter:

    # Initialize writer.
    def __init__(self, mode, table, key, ignore=None, hash=None, drop=None):
        validate_mode(mode=mode, valid=["append", "insert_update", "insert_update_delete"])
        self.key    = key
        self.drop   = drop
        self.hash   = hash
        self.mode   = mode
        self.table  = table
        self.ignore = ignore
    
    # Create Delta table with added metadata columns ANONYMIZED, LOADED, OPERATION, CHECKSUM and KEY.
    def __create_table(self, df):
        df = df.select([lit("C").alias(OPERATION), "*"])
        df = df.select([current_timestamp().alias(LOADED), "*"])
        df = df.select([current_timestamp().alias(ANONYMIZED), "*"])
        df = df.filter("1=0")
        dw = df.write.format("delta") \
            .clusterBy(KEY, LOADED, OPERATION) \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .option("delta.autoOptimize.autoCompact", "true")
        if EXTERNAL is not None:
            dw = dw.option("path", EXTERNAL + self.table.replace(".", "/"))
        dw.saveAsTable(self.table)
    
    # Prepare data frame and Delta table for writing.
    def __prepare(self, df):
        # Perform column transformations.
        df = conform_column_names(df)                      # 1 Conform column names
        if self.hash: df = add_hash_columns(df, self.hash) # 2 Add hash columns
        df = add_key_column(df, self.key)                  # 3 Add key column
        if self.drop: df = drop_columns(df, self.drop)     # 4 Drop columns
        df = add_checksum_column(df, self.ignore)          # 5 Add checksum column
        # Remove duplicates
        # Create table.
        if not spark.catalog._jcatalog.tableExists(self.table):
            self.__create_table(df)
            return df, True
        else:
            return df, False
    
    # Write data frame to Delta table.
    def write(self, df):
        df, new = self.__prepare(df)
        # mode = "append" if new else self.mode
        mode = self.mode
        if mode == "append":
            self.__append(df)
        elif mode == "insert_update":
            self.__insert_update(df)
        elif mode == "insert_update_delete":
            self.__insert_update_delete(df)
        else:
            raise Exception("Invalid mode")
        return get_num_output_rows(self.table)
    
    # Append source data frame.
    def __append(self, df):
        df = df.select([lit("I").alias(OPERATION), "*"])
        df = df.select([current_timestamp().alias(LOADED), "*"])
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(self.table)
    
    # Capture inserted and updated records from full or partial source data frame.
    # Will only write one row per key even if source data frame contains duplicate rows.
    # Arbitrary row will be used if source data frame contains nonidentical duplicate rows.
    def __insert_update(self, df):
        ws = Window.partitionBy(col(KEY)).orderBy(col("_origin"))
        df = df.withColumn("_origin", lit(1)) \
        .unionByName(
            read_active(self.table, [f"{KEY}", f"{CHECKSUM}"]).withColumn("_origin", lit(2)),
            allowMissingColumns = True) \
        .select([
            when((col("_origin") == 1) & (lead(col(KEY)).over(ws).isNull()), "I") \
            .when((col("_origin") == 1) & (lead(col("_origin")).over(ws) == 2) & (col(CHECKSUM) != lead(col(CHECKSUM)).over(ws)), "U") \
            .alias(OPERATION),
            current_timestamp().alias(LOADED),
            "*"]) \
        .drop(col("_origin")).where(col(OPERATION).isNotNull())
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(self.table)
    
    # Capture inserted, updated and deleted records from full source data frame.
    # Will only write one row per key even if source data frame contains duplicate rows.
    # Arbitrary row will be used if source data frame contains nonidentical duplicate rows.
    def __insert_update_delete(self, df):
        ws = Window.partitionBy(col(KEY)).orderBy(col("_origin"))
        df = df.withColumn("_origin", lit(1)) \
        .unionByName(
            read_active(self.table, f"* EXCEPT ({OPERATION}, {LOADED})").withColumn("_origin", lit(2)),
            allowMissingColumns = True) \
        .select([
            when((col("_origin") == 1) & (lead(col(KEY)).over(ws).isNull()), "I") \
            .when((col("_origin") == 1) & (lead(col("_origin")).over(ws) == 2) & (col(CHECKSUM) != lead(col(CHECKSUM)).over(ws)), "U") \
            .when((col("_origin") == 2) & (lag(col(KEY)).over(ws).isNull()), "D") \
            .alias(OPERATION),
            current_timestamp().alias(LOADED),
            "*"]) \
        .drop(col("_origin")).where(col(OPERATION).isNotNull())
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(self.table)


# Base JDBC reader.
class BaseJdbcReader:

    # Read from table using single query.
    def __read_single(self, table):
        return spark.read.jdbc(properties=self.properties, url=self.url, table=table, numPartitions=1)

    # Read from table using multiple parallel queries.
    def __read_parallel(self, table, parallel_column, parallel_number, lower_bound, upper_bound):
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
        return self.__read_single(f"({query}) AS q")

    # Read from table and return data frame containing only key columns.
    def read_key(self, table, key, where=None):
        if isinstance(key, str):
            df = self.__read_single(table).select(key)
        elif isinstance(key, list):
            df = self.__read_single(table).select(*key)
        else:
            raise Exception("Invalid key")
        return filter(df=df, where=where)

    # Read from table and return data frame.
    def read(self, table, exclude=None, where=None, parallel_column=None, parallel_number=None):
        if parallel_column is None and parallel_number is None:
            df = self.__read_single(table)
        elif parallel_column is not None and parallel_number is not None:
            bounds = self.get_bounds(table=table, parallel_column=parallel_column, where=where)
            df = self.__read_parallel(
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
            df = self.__read_single(table)
        elif parallel_column is not None and parallel_number is not None:
            bounds = self.get_bounds_equal_to(table=table, column=column, value=value, parallel_column=parallel_column, where=where)
            df = self.__read_parallel(
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
                df = self.__read_single(table)
            elif parallel_column is not None and parallel_number is not None:
                bounds = self.get_bounds_greater_than(table=table, column=column, value=value, parallel_column=parallel_column, where=where)
                df = self.__read_parallel(
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


# MySQL JDBC reader.
# https://dev.mysql.com/downloads/connector/j/
class MysqlReader(BaseJdbcReader):

    # Initialize reader.
    def __init__(self, host, port, database, username, password):
        self.url = f"jdbc:mysql://{host}:{port}/{database}?user={username}&password={password}"
        self.properties = {"driver": "com.mysql.jdbc.Driver"}


# PostgreSQL JDBC reader.
# https://jdbc.postgresql.org/download.html
class PostgresqlReader(BaseJdbcReader):

    # Initialize reader.
    def __init__(self, host, port, database, username, password):
        self.url = f"jdbc:postgresql://{host}:{port}/{database}?" + urlencode({"user" : username, "password" : password})
        self.properties = {"driver": "org.postgresql.Driver"}


# SQLServer JDBC reader.
# https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server?view=sql-server-ver15
class SqlserverReader(BaseJdbcReader):

    # Initialize reader.
    def __init__(self, host, port, database, username, password):
        self.url = f"jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=true"
        self.properties = {
            "driver"   : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "user"     : username,
            "password" : password
        }
