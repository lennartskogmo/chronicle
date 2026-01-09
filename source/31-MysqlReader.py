# MySQL JDBC reader.
# https://dev.mysql.com/downloads/connector/j/
class MysqlReader(BaseReader):

    # Initialize reader.
    def __init__(self, host, port, database, username, password):
        self.url = f"jdbc:mysql://{host}:{port}/{database}?user={username}&password={password}"
        self.properties = {"driver": "com.mysql.jdbc.Driver"}
        self.options = {}
