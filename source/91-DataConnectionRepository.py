class DataConnectionRepository:

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
