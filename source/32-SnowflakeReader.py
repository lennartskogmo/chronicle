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
