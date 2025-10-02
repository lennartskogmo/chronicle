# Snowflake Spark Connector reader.
# https://docs.snowflake.com/en/user-guide/spark-connector-databricks
class SnowflakeReader(BaseReader):

    # Initialize reader.
    def __init__(self, host, warehouse, database, username, key=None, password=None):
        options = {
            "sfURL"       : host,
            "sfWarehouse" : warehouse,
            "sfDatabase"  : database,
            "sfUser"      : username         
        }
        if key is not None:
            key = key.replace("-----BEGIN RSA PRIVATE KEY-----", "")
            key = key.replace("-----END RSA PRIVATE KEY-----", "")
            key = key.replace("\n", "")
            options["pem_private_key"] = key
        if password is not None:
            options["sfPassword"] = password
        self.options = options

    # Read from table using single query.
    def _read_single(self, table):
        return spark.read.format("snowflake").options(**self.options).option("dbtable", table).load()
