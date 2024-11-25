from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from os import environ
from re import match, sub
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
SNOWFLAKE  = environ.get("CHRONICLE_SNOWFLAKE") 

# Define metadata column names.
KEY        = "__key"        # The record primary key.
CHECKSUM   = "__checksum"   # The record checksum.
OPERATION  = "__operation"  # The type of operation that produced the record.
LOADED     = "__loaded"     # The time the record was loaded.
ANONYMIZED = "__anonymized" # Reserved for the time the record was anonymized.
