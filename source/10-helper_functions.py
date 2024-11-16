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
        if connection_with_secrets is None:
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
                if key == "Host"     : reader_arguments["host"]     = value
                if key == "Port"     : reader_arguments["port"]     = value
                if key == "Database" : reader_arguments["database"] = value
                if key == "Username" : reader_arguments["username"] = value
                if key == "Password" : reader_arguments["password"] = value
        return reader_arguments        
    else:
        raise Exception("Invalid connection with secrets")

# Return secret if value contains reference to secret, otherwise return value.
def resolve_secret(value):
    if isinstance(value, str) and value.startswith("<") and value.endswith(">"):
        return dbutils.secrets.get(scope="kv", key=value[1:-1])
    else:
        return value

# Validate mode against dynamic list of values.
def validate_mode(valid, mode):
    if mode not in valid:
        raise Exception("Invalid mode")
