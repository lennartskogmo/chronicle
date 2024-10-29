# Return new reader instance if argument is string, else act as pass-through function and return argument itself.
def get_reader(reader):
    if isinstance(reader, str):
        # Lookup connection details.
        connection_with_secrets = spark.table(CONNECTION).filter(f"ConnectionName == '{reader}'").collect()[0].asDict()
        for key, value in connection_with_secrets.items():
            if isinstance(value, str) and value.startswith("<") and value.endswith(">"):
                connection_with_secrets[key] = dbutils.secrets.get(scope="kv", key=value[1:-1])
        # Map connection details to reader constructor arguments.
        reader_arguments = {}
        for key, value in connection_with_secrets.items():
            if value is not None:
                if key == "Host"     : reader_arguments["host"]     = value
                if key == "Port"     : reader_arguments["port"]     = value
                if key == "Database" : reader_arguments["database"] = value
                if key == "Username" : reader_arguments["username"] = value
                if key == "Password" : reader_arguments["password"] = value
        # Instantiate reader.
        reader = globals()[connection_with_secrets["Reader"]]
        reader = reader(**reader_arguments)
    return reader

# Validate mode against dynamic list of values.
def validate_mode(valid, mode):
    if mode not in valid:
        raise Exception("Invalid mode")
