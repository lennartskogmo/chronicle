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
