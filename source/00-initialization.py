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
CHRONICLE  = "__chronicle"                     # The configuration schema.
CONNECTION = "__chronicle.connection"          # The data connection configuration table.
OBJECT     = "__chronicle.object"              # The data object configuration table.
EXTERNAL   = environ.get("CHRONICLE_EXTERNAL") # The path to external table storage location.

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
