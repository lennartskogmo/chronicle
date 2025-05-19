# Databricks notebook source
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from os import environ
from re import match, sub
from threading import Lock
from time import sleep
from urllib.parse import urlencode

from pyspark.sql.functions import arrays_overlap, coalesce, col, concat_ws, current_timestamp, expr, lag, lead, lit, lower, md5, row_number, when, xxhash64
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

# Perform additional initialization if library is imported as module.
if __name__ != "__main__":
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

# Define configuration locations.
CHRONICLE      = "__chronicle"                           # The configuration schema.
CONNECTION     = "__chronicle.connection"                # The data connection configuration table.
OBJECT         = "__chronicle.object"                    # The data object configuration table.
EXTERNAL_PATH  = environ.get("CHRONICLE_EXTERNAL_PATH")  # The path to external table storage location.
TEMPORARY_PATH = environ.get("CHRONICLE_TEMPORARY_PATH") # The path to temporary storage location.

# Define Snowflake compatibility mode.
SNOWFLAKE_COMPATIBILITY = environ.get("CHRONICLE_SNOWFLAKE_COMPATIBILITY")

# Define aws parameter store support.
PARAMETER_STORE = environ.get("CHRONICLE_PARAMETER_STORE")

# Define metadata column names.
KEY        = "__key"        # The record primary key.
CHECKSUM   = "__checksum"   # The record checksum.
OPERATION  = "__operation"  # The type of operation that produced the record.
LOADED     = "__loaded"     # The time the record was loaded.
ANONYMIZED = "__anonymized" # Reserved for the time the record was anonymized.

# Define Snowflake reserved words.
SNOWFLAKE_RESERVED = {
    "account", "all", "alter", "and", "any", "as", "asc", "between", "by", "case", "cast",
    "check", "column", "connect", "connection", "constraint", "create", "cross", "current",
    "current_date", "current_time", "current_timestamp", "current_user", "database",
    "delete", "desc", "distinct", "drop", "else", "exists", "false", "following", "for",
    "from", "full", "grant", "group", "gscluster", "having", "ilike", "in", "increment",
    "inner", "insert", "intersect", "into", "is", "issue", "join", "lateral", "left",
    "like", "limit", "localtime", "localtimestamp", "minus", "natural", "not", "null",
    "of", "on", "or", "order", "organization", "qualify", "regexp", "revoke", "right",
    "rlike", "row", "rows", "sample", "schema", "select", "set", "some", "start", "table",
    "tablesample", "then", "to", "trigger", "true", "try_cast", "union", "unique",
    "update", "using", "values", "view", "when", "whenever", "where", "window", "with"
}

# Initialize ssm client.
if PARAMETER_STORE is not None:
    import boto3
    aws_region    = spark.conf.get("spark.databricks.clusterUsageTags.region")
    boto3_session = boto3.Session(region_name = aws_region)
    SSM_CLIENT    = boto3_session.client('ssm')


# Return dictionary containing connection configuration or None if no connection was found.
def get_connection(connection):
    if isinstance(connection, str):
        connection = spark.table(CONNECTION).filter(lower("ConnectionName") == lower(lit(connection))).collect()
        connection = connection[0].asDict() if connection else None
        return connection
    else:
        raise Exception("Invalid connection")

# Return dictionary containing connection configuration including secrets or None if no connection was found.
def get_connection_with_secrets(connection):
    if isinstance(connection, str):
        connection = spark.table(CONNECTION).filter(lower("ConnectionName") == lower(lit(connection))).collect()
        connection = connection[0].asDict() if connection else None
        if connection is None:
            return None
    if isinstance(connection, dict):
        if not connection:
            return None
        connection_with_secrets = {}
        for key, value in connection.items():
            connection_with_secrets[key] = resolve_secret(value)
        return connection_with_secrets
    else:
        raise Exception("Invalid connection")

# Return dictionary containing object configuration or None if no object was found.
def get_object(object):
    if isinstance(object, str):
        object = spark.table(OBJECT).filter(lower("ObjectName") == lower(lit(object))).collect()
        object = object[0].asDict() if object else None
        return object
    else:
        raise Exception("Invalid object")

# Return new reader instance or None if no reader was found.
def get_reader(connection):
    if isinstance(connection, str) or isinstance(connection, dict):
        connection_with_secrets = get_connection_with_secrets(connection)
        if connection_with_secrets is None or "Reader" not in connection_with_secrets:
            return None
        reader_arguments = map_reader_arguments(connection_with_secrets)
        reader = globals()[connection_with_secrets["Reader"]]
        return reader(**reader_arguments)
    else:
        raise Exception("Invalid connection")

# Map connection configuration with secrets to reader constructor arguments.
def map_reader_arguments(connection_with_secrets):
    if isinstance(connection_with_secrets, dict):
        reader_arguments = {}
        for key, value in connection_with_secrets.items():
            if value is not None:
                if key == "Host"      : reader_arguments["host"]      = value
                if key == "Port"      : reader_arguments["port"]      = value
                if key == "Database"  : reader_arguments["database"]  = value
                if key == "Username"  : reader_arguments["username"]  = value
                if key == "Password"  : reader_arguments["password"]  = value
                if key == "Warehouse" : reader_arguments["warehouse"] = value
        return reader_arguments
    else:
        raise Exception("Invalid connection with secrets")

# Map object configuration to function arguments.
def map_function_arguments(object):
    if isinstance(object, dict):
        if "Function" not in object or not object["Function"].startswith("load_"):
            raise Exception("Invalid function")
        function_arguments = {}
        for key, value in object.items():
            if value is not None:
                if key == "Mode"            : function_arguments["mode"]            = value
                if key == "ObjectName"      : function_arguments["target"]          = value
                if key == "ObjectSource"    : function_arguments["source"]          = value
                if key == "KeyColumns"      : function_arguments["key"]             = value
                if key == "ExcludeColumns"  : function_arguments["exclude"]         = value
                if key == "IgnoreColumns"   : function_arguments["ignore"]          = value
                if key == "HashColumns"     : function_arguments["hash"]            = value
                if key == "DropColumns"     : function_arguments["drop"]            = value
                if key == "BookmarkColumn"  : function_arguments["bookmark_column"] = value
                if key == "BookmarkOffset"  : function_arguments["bookmark_offset"] = value
                if key == "PartitionColumn" : function_arguments["parallel_column"] = value
                if key == "PartitionNumber" : function_arguments["parallel_number"] = value
        return function_arguments
    else:
        raise Exception("Invalid object")

# Parse tag string and return tag list.
def parse_tag(tag):
    if isinstance(tag, str):
        tag = tag.strip()
        tag = sub(r"\s+", ",", tag)             # Replace multiple spaces with a single comma.
        tag = sub(r",+", ",", tag)              # Replace multiple commas with a single comma.
        tag = sub(r"[^A-Za-z0-9_,]+", "", tag)  # Remove everything except alphanumeric characters, underscores and commas.
        tag = tag.split(",")
        return tag
    else:
        raise Exception("Invalid tag")

# Return secret if value contains reference to secret, otherwise return value.
def resolve_secret(value):
    if isinstance(value, str) and value.startswith("<") and value.endswith(">"):
        if PARAMETER_STORE is not None:
            return SSM_CLIENT.get_parameter(Name=value[1:-1], WithDecryption=True)['Parameter']['Value']
        else:
            return dbutils.secrets.get(scope="kv", key=value[1:-1])
    else:
        return value

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
    return df.select([xxhash64(concat_ws("<|>", *[coalesce(col(c).cast(StringType()), lit("")) for c in columns])).alias(CHECKSUM), "*"])

# Add KEY column to beginning of data frame.
def add_key_column(df, key):
    if isinstance(key, str):
        return df.select([df[conform_column_name(key)].alias(KEY), "*"])
    elif isinstance(key, list) and len(key) == 1:
        return df.select([df[conform_column_name(key[0])].alias(KEY), "*"])
    elif isinstance(key, list) and len(key) > 1:
        key = sorted([conform_column_name(c) for c in key])
        return df.select([concat_ws("<|>", *[coalesce(col(c).cast(StringType()), lit("")) for c in key]).alias(KEY), "*"])
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
    cn = cn.translate(str.maketrans("ÆØÅæøå", "EOAeoa")) # Replace Norwegian characters with Latin characters.
    if cn != cn.upper():
        cn = sub("([A-Z])", r"_\1", cn)                  # Add underscore before upper case letters if column name is mixed case.
    cn = cn.lower()                                      # Convert to lower case.
    cn = sub("[^a-z0-9]+", "_", cn)                      # Replace all characters except letters and numbers with underscores.
    cn = sub("^_|_$", "", cn)                            # Remove leading and trailing underscores.
    if SNOWFLAKE_COMPATIBILITY is not None:
        if cn in SNOWFLAKE_RESERVED:
            cn += "_"                                    # Add underscore postfix if column name is a snowflake reserved word. 
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


# Load object according to configuration.
# Can either look up configuration based on object name or use configuration supplied through arguments.
def load_object(object, connection=None, connection_with_secrets=None):
    # Prepare object configuration.
    if isinstance(object, str):
        object = get_object(object)
    if not isinstance(object, dict):
        raise Exception("Invalid object")
    # Prepare connection configuration.
    if connection is None:
        connection = get_connection(object["ConnectionName"])
    if not isinstance(connection, dict):
        raise Exception("Invalid connection")
    # Prepare connection configuration with secrets.
    if connection_with_secrets is None:
        connection_with_secrets = get_connection_with_secrets(connection)
    if not isinstance(connection_with_secrets, dict):
        raise Exception("Invalid connection with secrets")
    
    # Map connection configuration with secrets to reader constructor arguments.
    reader_arguments = map_reader_arguments(connection_with_secrets)
    # Map object configuration to function arguments.
    function_arguments = map_function_arguments(object)

    # Instantiate reader if connection is associated with reader.
    if "Reader" in connection_with_secrets and connection_with_secrets["Reader"] is not None:
        reader = globals()[connection_with_secrets["Reader"]]
        function_arguments["reader"] = reader(**reader_arguments)
    
    # Invoke function.
    function = globals()[object["Function"]]
    return function(**function_arguments)

# Perform full load from source to target.
def load_full(
        reader, source, target, key, mode="insert_update_delete",
        exclude=None, ignore=None, hash=None, drop=None, where=None, parallel_number=None, parallel_column=None
    ):
    validate_mode(mode=mode, valid=["insert_update", "insert_update_delete"])
    writer = DeltaBatchWriter(mode=mode, table=target, key=key, ignore=ignore, hash=hash, drop=drop)
    df = reader.read(table=source, exclude=exclude, where=where, parallel_number=parallel_number, parallel_column=parallel_column)
    return writer.write(df)

# Perform incremental load from source to target.
def load_incremental(
        reader, source, target, key, bookmark_column, bookmark_offset=None, mode="insert_update",
        exclude=None, ignore=None, hash=None, drop=None, where=None, parallel_number=None, parallel_column=None
    ):
    validate_mode(mode=mode, valid=["insert_update"])
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
        if EXTERNAL_PATH is not None:
            dw = dw.option("path", EXTERNAL_PATH + self.table.replace(".", "/"))
        if SNOWFLAKE_COMPATIBILITY is not None:
            dw = dw.option("delta.checkpointPolicy", "classic")
            dw = dw.option("delta.enableDeletionVectors", "false")
            dw = dw.option("delta.enableRowTracking", "false")
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


# Base reader.
class BaseReader:

    # Read from table using single query.
    def _read_single(self, table):
        return spark.read.jdbc(
            properties    = self.properties,
            url           = self.url,
            table         = table,
            numPartitions = 1
        )

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


# MySQL JDBC reader.
# https://dev.mysql.com/downloads/connector/j/
class MysqlReader(BaseReader):

    # Initialize reader.
    def __init__(self, host, port, database, username, password):
        self.url = f"jdbc:mysql://{host}:{port}/{database}?user={username}&password={password}"
        self.properties = {"driver": "com.mysql.jdbc.Driver"}


# PostgreSQL JDBC reader.
# https://jdbc.postgresql.org/download.html
class PostgresqlReader(BaseReader):

    # Initialize reader.
    def __init__(self, host, port, database, username, password):
        self.url = f"jdbc:postgresql://{host}:{port}/{database}?" + urlencode({"user" : username, "password" : password})
        self.properties = {"driver": "org.postgresql.Driver"}


# SQLServer JDBC reader.
# https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server?view=sql-server-ver15
class SqlserverReader(BaseReader):

    # Initialize reader.
    def __init__(self, host, port, database, username, password):
        self.url = f"jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=true"
        self.properties = {
            "driver"   : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "user"     : username,
            "password" : password
        }


# Snowflake Spark Connector reader.
# https://docs.snowflake.com/en/user-guide/spark-connector-databricks
class SnowflakeReader(BaseReader):

    # Initialize reader.
    def __init__(self, host, warehouse, database, username, password):
        self.options = {
            "sfURL"       : host,
            "sfWarehouse" : warehouse,
            "sfDatabase"  : database,
            "sfUser"      : username,
            "sfPassword"  : password         
        }

    # Read from table using single query.
    def _read_single(self, table):
        return spark.read.format("snowflake").options(**self.options).option("dbtable", table).load()


class ObjectLoader2:

    def __init__(self, concurrency, tags, post_hook=None):
        self.lock      = Lock()
        self.executor  = ThreadPoolExecutor(max_workers=int(concurrency))
        self.queue     = ObjectLoaderQueue2(concurrency=int(concurrency), tags=tags)
        self.post_hook = post_hook

    def run(self):
        self.__log(f"Concurrency : {self.queue.global_maximum_concurrency}")
        self.__log(f"Connections : {len(self.queue.queued)}")
        self.__log(f"Objects : {self.queue.length}\n")
        futures = []
        while self.queue.not_empty():
            # Get eligible objects including connection details from queue and submit them to executor.
            while object := self.queue.get():
                futures.append(self.executor.submit(self.__load_object, object))
            # Check for completed futures and report back to queue.
            completed = 0
            for i, future in enumerate(futures):
                if future.done():
                    future = futures.pop(i)
                    self.queue.complete(future.result())
                    completed += 1
            if completed > 0:
                # Resort queue to maximize concurrency utilization.
                self.queue.sort()
            # Take a short nap so the main thread is not constantly calling queue.not_empty() and queue.get().
            sleep(0.1)

    def __load_object(self, object):
        attempt = 0
        start = datetime.now()
        while True:
            try:
                attempt += 1
                if attempt == 1:
                    self.__log(f"{start.time().strftime('%H:%M:%S')}  [Starting]   {object.ObjectName}")
                else:
                    self.__log(f"{datetime.now().time().strftime('%H:%M:%S')}  [Retrying]   {object.ObjectName}")
                object.loader_result = object.load()
                if self.post_hook is not None:
                    try:
                        self.post_hook(object)
                    except Exception as e:
                        object.loader_exception = e
                break
            except Exception as e:
                if attempt >= 2:
                    object.loader_exception = e
                    break
                sleep(5)
        end = datetime.now()
        object.loader_duration = int(round((end - start).total_seconds(), 0))
        if hasattr(object, "loader_exception"):
            object.loader_status = "Failed"
            self.__log(f"{end.time().strftime('%H:%M:%S')}  [Failed]     {object.ObjectName} ({object.loader_duration} Seconds) ({attempt} Attempts)")
        else:
            object.loader_status = "Completed"
            self.__log(f"{end.time().strftime('%H:%M:%S')}  [Completed]  {object.ObjectName} ({object.loader_duration} Seconds) ({object.loader_result} Rows)")
        return object

    def __log(self, message):
        self.lock.acquire()
        print(message)
        self.lock.release()

    def print_errors(self):
        failed = 0
        for object in self.queue.completed.values():
            if object.loader_status == "Failed":
                failed += 1
                self.__log(f"{object.ObjectName}:")
                self.__log(object.loader_exception)
        if failed == 0:
            self.__log("No errors")


class ObjectLoader:

    def __init__(self, concurrency, tag, post_hook=None):
        self.lock      = Lock()
        self.executor  = ThreadPoolExecutor(max_workers=int(concurrency))
        self.queue     = ObjectLoaderQueue(concurrency=int(concurrency), tag=tag)
        self.post_hook = post_hook

    def run(self):
        self.__log(f"Concurrency : {self.queue.global_maximum_concurrency}")
        self.__log(f"Connections : {len(self.queue.connections)}")
        self.__log(f"Objects : {self.queue.length}\n")
        futures = []
        while self.queue.not_empty():
            # Get eligible objects including connection details from queue and submit them to executor.
            while object := self.queue.get():
                connection = self.queue.connections[object["ConnectionName"]]
                connection_with_secrets = self.queue.connections_with_secrets[object["ConnectionName"]]
                futures.append(self.executor.submit(self.__load_object, object, connection, connection_with_secrets))
            # Check for completed futures and report back to queue.
            completed = 0
            for i, future in enumerate(futures):
                if future.done():
                    future = futures.pop(i)
                    self.queue.complete(future.result())
                    completed += 1
            if completed > 0:
                # Resort queue to maximize concurrency utilization.
                self.queue.sort()
            # Take a short nap so the main thread is not constantly calling queue.not_empty() and queue.get().
            sleep(0.1)
    
    def __load_object(self, object, connection, connection_with_secrets):
        attempt = 0
        start = datetime.now()
        while True:
            try:
                attempt += 1
                if attempt == 1:
                    self.__log(f"{start.time().strftime('%H:%M:%S')}  [Starting]   {object['ObjectName']}")
                else:
                    self.__log(f"{datetime.now().time().strftime('%H:%M:%S')}  [Retrying]   {object['ObjectName']}")
                object["__rows"] = load_object(object, connection, connection_with_secrets)
                if self.post_hook is not None:
                    try:
                        self.post_hook(object)
                    except Exception as e:
                        object["__exception"] = e
                break
            except Exception as e:
                if attempt >= 2:
                    object["__exception"] = e
                    break
                sleep(5)
        end = datetime.now()
        object["__duration"] = int(round((end - start).total_seconds(), 0))
        if "__exception" in object:
            object["__status"] = "Failed"
            self.__log(f"{end.time().strftime('%H:%M:%S')}  [Failed]     {object['ObjectName']} ({object['__duration']} Seconds) ({attempt} Attempts)")
        else:
            object["__status"] = "Completed"
            self.__log(f"{end.time().strftime('%H:%M:%S')}  [Completed]  {object['ObjectName']} ({object['__duration']} Seconds) ({object['__rows']} Rows)")
        return object
    
    def __log(self, message):
        self.lock.acquire()
        print(message)
        self.lock.release()

    def print_errors(self):
        failed = 0
        for object in self.queue.completed.values():
            if object["__status"] == "Failed":
                failed += 1
                self.__log(f"{object['ObjectName']}:")
                self.__log(object["__exception"])
        if failed == 0:
            self.__log("No errors")


class ObjectLoaderQueue2:

    # Initialize queue.
    def __init__(self, concurrency, tags):
        objects     = get_objects().active().tags(tags)
        connections = objects.get_connections()
        self.length = len(objects)

        # Initialize concurrency.
        self.global_maximum_concurrency = concurrency
        self.global_current_concurrency = 0
        self.connection_maximum_concurrency = {}
        self.connection_current_concurrency = {}
        for connection_name, connection in connections.items():
            self.connection_maximum_concurrency[connection_name] = connection.ConcurrencyLimit
            self.connection_current_concurrency[connection_name] = 0

        # Initalize object dictionaries.
        queued = {}
        for connection_name, connection in connections.items():
            queued[connection_name] = {"ConcurrencyLimit" : connection.ConcurrencyLimit, "Objects" : {}}
        for object in objects.values():
            queued[object.ConnectionName]["Objects"][object.ObjectName] = object
        for connection_name, connection in queued.items():
            queued[connection_name]["Objects"] = dict(sorted(connection["Objects"].items(), key=lambda item: item[1].ConcurrencyNumber, reverse=True))
        self.queued    = queued
        self.completed = {}
        self.started   = {}
        self.sort()

    # Return next eligible object.
    def get(self):
        # Find next eligible object.
        object_name = None
        if self.global_current_concurrency < self.global_maximum_concurrency:
            for connection_name, connection in self.queued.items():
                if self.connection_current_concurrency[connection_name] < self.connection_maximum_concurrency[connection_name]:
                    if connection["Objects"]:
                        object_name = list(connection["Objects"].keys())[0]
                        break
        # Register object as started and return object.
        if object_name:
            object = self.queued[connection_name]["Objects"].pop(object_name)
            self.started[object_name] = object
            self.connection_current_concurrency[connection_name] += 1
            self.global_current_concurrency += 1
            return object

    # Register object as completed.
    def complete(self, object):
        object_name = object.ObjectName
        connection_name = object.ConnectionName
        self.started.pop(object_name)
        self.completed[object_name] = object
        self.connection_current_concurrency[connection_name] -= 1
        self.global_current_concurrency -= 1
        self.length -= 1

    # Return true until all objects in queue have been processed.
    def not_empty(self):
        if self.length + len(self.started) > 0:
            return True

    # Sort queue so objects will be picked from connections that require the longest remaining time first.
    def sort(self):
        # Calculate score per connection.
        score = {}
        for connection_name, connection in self.queued.items():
            length = len(connection["Objects"])
            if length > 0:
                score[connection_name] = length / connection["ConcurrencyLimit"]
            else:
                score[connection_name] = 0
        # Replace queue with new queue sorted by descending connection score.
        connection_names = sorted(score, key=score.get, reverse=True)
        queued = {}
        for connection_name in connection_names:
            queued[connection_name] = self.queued[connection_name]
        self.queued = queued


class ObjectLoaderQueue:

    length    = 0
    queued    = {}
    started   = {}
    completed = {}
    connections              = {}
    connections_with_secrets = {}
    global_maximum_concurrency = 0
    global_current_concurrency = 0
    connection_maximum_concurrency = {}
    connection_current_concurrency = {}

    # Initialize queue.
    def __init__(self, concurrency, tag):
        self.global_maximum_concurrency = concurrency
        self.__populate(tag)
        self.sort()
        for connection_name, connection in self.queued.items():
            self.connection_maximum_concurrency[connection_name] = connection["ConcurrencyLimit"]
            self.connection_current_concurrency[connection_name] = 0
            self.length += len(connection["Objects"])

    # Populate queue with objects from database.
    def __populate(self, tag):
        if isinstance(tag, str):
            tag = parse_tag(tag)
        if not isinstance(tag, list):
            raise Exception("Invalid tag")
        objects = spark.table(OBJECT).withColumn("__Tags", lit(tag)).where("Status = 'Active'").filter(arrays_overlap("Tags", "__Tags")).drop("__Tags")
        connections = spark.table("__chronicle.connection").join(objects, ["ConnectionName"], "leftsemi")
        objects = {row["ObjectName"] : row.asDict() for row in objects.collect()}
        connections = {row["ConnectionName"] : row.asDict() for row in connections.collect()}
        # Set object concurrency number.
        for object_name, object in objects.items():
            concurrency_number = 1
            if "ConcurrencyNumber" in object and isinstance(object["ConcurrencyNumber"], int) and object["ConcurrencyNumber"] > concurrency_number:
                concurrency_number = object["ConcurrencyNumber"]
            if "PartitionNumber" in object and isinstance(object["PartitionNumber"], int) and object["PartitionNumber"] > concurrency_number:
                concurrency_number = object["PartitionNumber"]
            objects[object_name]["ConcurrencyNumber"] = concurrency_number
        # Prepare dictionaries containing connection details with and without secrets.
        for connection_name, connection in connections.items():
            self.connections[connection_name] = {}
            self.connections_with_secrets[connection_name] = {}
            for key, value in connection.items():
                self.connections[connection_name][key] = value
                self.connections_with_secrets[connection_name][key] = resolve_secret(value)
        # Prepare nested queued dictionary containing objects to be processed.
        for connection_name in connections.keys():
            connections[connection_name]["Objects"] = {}
        for object in objects.values():
            connections[object["ConnectionName"]]["Objects"][object["ObjectName"]] = object
        for connection_name, connection in connections.items():
            connections[connection_name]["Objects"] = dict(sorted(connection["Objects"].items(), key=lambda item: item[1]['ConcurrencyNumber'], reverse=True))
        self.queued = connections

    # Return next eligible object.
    def get(self):
        # Find next eligible object.
        object_name = None
        if self.global_current_concurrency < self.global_maximum_concurrency:
            for connection_name, connection in self.queued.items():
                if self.connection_current_concurrency[connection_name] < self.connection_maximum_concurrency[connection_name]:
                    if connection["Objects"]:
                        object_name = list(connection["Objects"].keys())[0]
                        break
        # Register object as started and return object.
        if object_name:
            object = self.queued[connection_name]["Objects"].pop(object_name)
            self.started[object_name] = object
            self.connection_current_concurrency[connection_name] += 1
            self.global_current_concurrency += 1
            return object

    # Register object as completed.
    def complete(self, object):
        object_name = object["ObjectName"]
        connection_name = object["ConnectionName"]
        self.started.pop(object_name)
        self.completed[object_name] = object
        self.connection_current_concurrency[connection_name] -= 1
        self.global_current_concurrency -= 1
        self.length -= 1

    # Return true until all objects in queue have been processed.
    def not_empty(self):
        if self.length + len(self.started) > 0:
            return True

    # Sort queue so objects will be picked from connections that require the longest remaining time first.
    def sort(self):
        # Calculate score per connection.
        score = {}
        for connection_name, connection in self.queued.items():
            length = len(connection["Objects"])
            if length > 0:
                score[connection_name] = length / connection["ConcurrencyLimit"]
            else:
                score[connection_name] = 0
        # Replace queue with new queue sorted by descending connection score.
        connection_names = sorted(score, key=score.get, reverse=True)
        queued = {}
        for connection_name in connection_names:
            queued[connection_name] = self.queued[connection_name]
        self.queued = queued


class DataConnection:

    # Initialize connection.
    def __init__(self, configuration):
        if not isinstance(configuration, dict):
            raise Exception("Invalid configuration")
        for key, value in configuration.items():
            setattr(self, key, value)
        self.__validate_configuration()
        self.__lock = Lock()

    # Validate configuration attributes.
    def __validate_configuration(self):
        # Validate mandatory ConnectionName.
        if not hasattr(self, "ConnectionName"):
            raise Exception(f"Missing ConnectionName in {vars(self)}")
        if not isinstance(self.ConnectionName, str) or self.ConnectionName.strip() == "":
            raise Exception(f"Invalid ConnectionName in {vars(self)}")

        # Validate mandatory ConcurrencyLimit.
        if not hasattr(self, "ConcurrencyLimit"):
            raise Exception(f"Missing ConcurrencyLimit in {self.ConnectionName}")
        if not isinstance(self.ConcurrencyLimit, int) or self.ConcurrencyLimit < 1:
            raise Exception(f"Invalid ConcurrencyLimit in {self.ConnectionName}")

        # Validate optional Reader.
        if hasattr(self, "Reader") and self.Reader is not None and not isinstance(self.Reader, str):
            raise Exception(f"Invalid Reader in {self.ConnectionName}")

    # Return dictionary containing configuration with secrets.
    def __get_configuration_with_secrets(self):
        # Resolve secrets and initialize dictionary the first time method is called.
        self.__lock.acquire()
        if not hasattr(self, f"_{self.__class__.__name__}__configuration_with_secrets"):
            configuration_with_secrets = {}
            for key, value in vars(self).items():
                if value is not None:
                    value = resolve_secret(value)
                    # Wrap value in function.
                    def get_value(value=value):
                        return value
                    configuration_with_secrets[key] = get_value
            self.__configuration_with_secrets = configuration_with_secrets
        self.__lock.release()
        return self.__configuration_with_secrets

    # Return new reader instance.
    def get_reader(self):
        if not hasattr(self, "Reader") or self.Reader is None:
            raise Exception(f"Connection {self.ConnectionName} has no reader")
        configuration_with_secrets = self.__get_configuration_with_secrets()
        reader = globals()[configuration_with_secrets["Reader"]()]
        reader_arguments = {}
        # Map connection configuration with secrets to reader constructor arguments.
        for key, value in configuration_with_secrets.items():
            if key == "Host"      : reader_arguments["host"]      = value()
            if key == "Port"      : reader_arguments["port"]      = value()
            if key == "Database"  : reader_arguments["database"]  = value()
            if key == "Username"  : reader_arguments["username"]  = value()
            if key == "Password"  : reader_arguments["password"]  = value()
            if key == "Warehouse" : reader_arguments["warehouse"] = value()
        return reader(**reader_arguments)

    # Return true if connection has reader else return false.
    def has_reader(self):
        return True if hasattr(self, "Reader") and self.Reader is not None else False


class DataConnectionRepository: # [OK]

    __instance = None

    # Implement singleton behaviour.
    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
        return cls.__instance

    # Initialize repository.
    def __init__(self):
        # Read data from Delta table.
        connections = spark.table(CONNECTION)
        # Instantiate connections.
        self.__connections = {row["ConnectionName"] : DataConnection(row.asDict()) for row in connections.collect()}

    # Return connection or None if connection does not exist.
    def get_connection(self, connection_name):
        if not isinstance(connection_name, str) or connection_name.strip() == "":
            raise Exception("Invalid connection name")
        return self.__connections.get(connection_name)


class DataObject:

    # Initialize object.
    def __init__(self, configuration):
        if not isinstance(configuration, dict):
            raise Exception("Invalid configuration")
        for key, value in configuration.items():
            setattr(self, key, value)
        self.__validate_configuration()
        self.__set_concurrency_number()

    # Validate configuration attributes.
    def __validate_configuration(self):
        # Validate mandatory ObjectName.
        if not hasattr(self, "ObjectName"):
            raise Exception(f"Missing ObjectName in {vars(self)}")
        if not isinstance(self.ObjectName, str) or self.ObjectName.strip() == "":
            raise Exception(f"Invalid ObjectName in {vars(self)}")

        # Validate mandatory ConnectionName.
        if not hasattr(self, "ConnectionName"):
            raise Exception(f"Missing ConnectionName in {self.ObjectName}")
        if not isinstance(self.ConnectionName, str) or self.ConnectionName.strip() == "":
            raise Exception(f"Invalid ConnectionName in {self.ObjectName}")

        # Validate mandatory Function.
        if not hasattr(self, "Function"):
            raise Exception(f"Missing Function in {self.ObjectName}")
        if not isinstance(self.Function, str) or self.Function.strip() == "" or not self.Function.startswith("load_"):
            raise Exception(f"Invalid Function in {self.ObjectName}")

        # Validate mandatory Status.
        if not hasattr(self, "Status"):
            raise Exception(f"Missing Status in {self.ObjectName}")
        if not isinstance(self.Status, str) or not self.Status in ["Active", "Inactive"]:
            raise Exception(f"Invalid Status in {self.ObjectName}")

        # Validate optional ConcurrencyNumber.
        if hasattr(self, "ConcurrencyNumber") and self.ConcurrencyNumber is not None and not isinstance(self.ConcurrencyNumber, int):
            raise Exception(f"Invalid ConcurrencyNumber in {self.ObjectName}")

        # Validate optional PartitionNumber.
        if hasattr(self, "PartitionNumber") and self.PartitionNumber is not None and not isinstance(self.PartitionNumber, int):
            raise Exception(f"Invalid PartitionNumber in {self.ObjectName}")

        # Validate optional Tags.
        if hasattr(self, "Tags") and self.Tags is not None and not isinstance(self.Tags, list):
            raise Exception(f"Invalid Tags in {self.ObjectName}")

    # Set concurrency number to 1 or the greatest of concurrency number and partition number.
    def __set_concurrency_number(self):
        concurrency_number = 1
        if hasattr(self, "ConcurrencyNumber") and self.ConcurrencyNumber is not None and self.ConcurrencyNumber > concurrency_number:
            concurrency_number = self.ConcurrencyNumber
        if hasattr(self, "PartitionNumber") and self.PartitionNumber is not None and self.PartitionNumber > concurrency_number:
            concurrency_number = self.PartitionNumber
        self.ConcurrencyNumber = concurrency_number

    # Inject connection.
    def set_connection(self, connection):
        if not isinstance(connection, DataConnection):
            raise Exception("Invalid connection")
        if hasattr(self, f"_{self.__class__.__name__}__connection"):
            raise Exception("Connection already set")
        self.__connection = connection

    # Return connection.
    def get_connection(self):
        return self.__connection

    # Load object.
    def load(self):
        function = globals()[self.Function]
        function_arguments = {}
        # Map object configuration to load function arguments.
        for key, value in vars(self).items():
            if value is not None:
                if key == "Mode"            : function_arguments["mode"]            = value
                if key == "ObjectName"      : function_arguments["target"]          = value
                if key == "ObjectSource"    : function_arguments["source"]          = value
                if key == "KeyColumns"      : function_arguments["key"]             = value
                if key == "ExcludeColumns"  : function_arguments["exclude"]         = value
                if key == "IgnoreColumns"   : function_arguments["ignore"]          = value
                if key == "HashColumns"     : function_arguments["hash"]            = value
                if key == "DropColumns"     : function_arguments["drop"]            = value
                if key == "BookmarkColumn"  : function_arguments["bookmark_column"] = value
                if key == "BookmarkOffset"  : function_arguments["bookmark_offset"] = value
                if key == "PartitionColumn" : function_arguments["parallel_column"] = value
                if key == "PartitionNumber" : function_arguments["parallel_number"] = value
        if self.__connection.has_reader():
            function_arguments["reader"] = self.__connection.get_reader()
        return function(**function_arguments)


class DataObjectCollection:

    # Initialize collection.
    def __init__(self, objects):
        if not isinstance(objects, dict):
            raise Exception("Invalid objects")
        for object_name, object in objects.items():
            if not isinstance(object, DataObject):
                raise Exception(f"Invalid object {object_name}")
        self.__objects = objects

    def __getitem__(self, object_name):
        return self.__objects.get(object_name)

    def __len__(self):
        return len(self.__objects)

    def __repr__(self):
        return "\n".join([str(key) for key in self.__objects.keys()])

    def items(self):
        return self.__objects.items()

    def keys(self):
        return self.__objects.keys()

    def values(self):
        return self.__objects.values()

    # Print list of all objects in collection.
    def list(self):
        print(len(self))
        print(self)

    # Return dictionary of connections associated with objects in the collection.
    def get_connections(self):
        connections = {}
        for object_name, object in self.__objects.items():
            connection = object.get_connection()
            connections[connection.ConnectionName] = connection
        return connections

    # Return new subcollection containing only active objects.
    def active(self):
        objects = {}
        for object_name, object in self.__objects.items():
            if object.Status == "Active":
                objects[object_name] = object
        return self.__class__(objects)

    # Return new subcollection containing only inactive objects.
    def inactive(self):
        objects = {}
        for object_name, object in self.__objects.items():
            if object.Status == "Inactive":
                objects[object_name] = object
        return self.__class__(objects)

    # Return new subcollection containing only objects with matching connection name.
    def connection(self, connection_name):
        if not isinstance(connection_name, str):
            raise Exception("Invalid connection name")
        objects = {}
        for object_name, object in self.__objects.items():
            if object.ConnectionName == connection_name:
                objects[object_name] = object
        return self.__class__(objects)

    # Return new subcollection containing only objects with atleast one matching tag.
    def tags(self, tags):
        if isinstance(tags, str):
            tags = parse_tag(tags)
        if not isinstance(tags, list):
            raise Exception("Invalid tags")
        objects = {}
        for object_name, object in self.__objects.items():
            if hasattr(object, "Tags") and any(item in object.Tags for item in tags):
                objects[object_name] = object
        return self.__class__(objects)


class DataObjectRepository: # [OK]

    __instance = None

    # Implement singleton behaviour.
    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
        return cls.__instance

    # Initialize repository.
    def __init__(self):
        repo = DataConnectionRepository()
        # Read data from Delta table.
        objects = spark.table(OBJECT)
        # Instantiate objects.
        objects = {row["ObjectName"] : DataObject(row.asDict()) for row in objects.collect()}
        for object_name, object in objects.items():
            object.set_connection(repo.get_connection(object.ConnectionName))
        # Instantiate collection.
        self.__collection = DataObjectCollection(objects)

    # Return object or None if object does not exist.
    def get_object(self, object_name):
        if not isinstance(object_name, str) or object_name.strip() == "":
            raise Exception("Invalid object name")
        return self.__collection[object_name]

    # Return collection containing all objects.
    def get_objects(self):
        return self.__collection


def get_objects():
    repo = DataObjectRepository()
    return repo.get_objects()
