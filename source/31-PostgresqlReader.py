# PostgreSQL JDBC reader.
# https://jdbc.postgresql.org/download.html
class PostgresqlReader(BaseJdbcReader):

    # Initialize reader.
    def __init__(self, host, port, database, username, password):
        self.url = f"jdbc:postgresql://{host}:{port}/{database}?" + urlencode({"user" : username, "password" : password})
        self.properties = {"driver": "org.postgresql.Driver"}
