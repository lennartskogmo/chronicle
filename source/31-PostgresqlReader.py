# PostgreSQL JDBC reader.
# https://jdbc.postgresql.org/download.html
class PostgresqlReader(BaseReader):

    # Initialize reader.
    def __init__(self, host, port, database, username, password, timestamp_ntz = None):
        self.url = f"jdbc:postgresql://{host}:{port}/{database}?" + urlencode({"user" : username, "password" : password})
        self.properties = {"driver": "org.postgresql.Driver"}
        self.options = {}
        if timestamp_ntz is True or (isinstance(timestamp_ntz, str) and timestamp_ntz.lower() == "true"):
            self.options["preferTimestampNTZ"] = "true"
