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
        if connection_with_secrets is None or "Reader" not in connection_with_secrets:
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
                if key == "Host"      : reader_arguments["host"]      = value
                if key == "Port"      : reader_arguments["port"]      = value
                if key == "Database"  : reader_arguments["database"]  = value
                if key == "Username"  : reader_arguments["username"]  = value
                if key == "Password"  : reader_arguments["password"]  = value
                if key == "Warehouse" : reader_arguments["warehouse"] = value
        return reader_arguments
    else:
        raise Exception("Invalid connection with secrets")

# Map object configuration to function arguments.
def map_function_arguments(object):
    if isinstance(object, dict):
        if "Function" not in object or not object["Function"].startswith("load_"):
            raise Exception("Invalid function")
        function_arguments = {}
        for key, value in object.items():
            if value is not None:
                if key == "Mode"           : function_arguments["mode"]            = value
                if key == "ObjectName"     : function_arguments["target"]          = value
                if key == "ObjectSource"   : function_arguments["source"]          = value
                if key == "KeyColumns"     : function_arguments["key"]             = value
                if key == "ExcludeColumns" : function_arguments["exclude"]         = value
                if key == "IgnoreColumns"  : function_arguments["ignore"]          = value
                if key == "HashColumns"    : function_arguments["hash"]            = value
                if key == "DropColumns"    : function_arguments["drop"]            = value
                if key == "BookmarkColumn" : function_arguments["bookmark_column"] = value
                if key == "BookmarkOffset" : function_arguments["bookmark_offset"] = value
        return function_arguments
    else:
        raise Exception("Invalid object")

# Parse tag string and return tag list.
def parse_tag(tag):
    if isinstance(tag, str):
        tag = tag.strip()
        tag = sub(r"\s+", ",", tag)             # Replace multiple spaces with a single comma.
        tag = sub(r",+", ",", tag)              # Replace multiple commas with a single comma.
        tag = sub(r"[^A-Za-z0-9_,]+", "", tag)  # Remove everything except alphanumeric characters, underscores and commas.
        tag = tag.split(",")
        return tag
    else:
        raise Exception("Invalid tag")

# Return secret if value contains reference to secret, otherwise return value.
def resolve_secret(value):
    if isinstance(value, str) and value.startswith("<") and value.endswith(">"):
        if PARAMETER_STORE is not None:
            return SSM_CLIENT.get_parameter(Name=value[1:-1], WithDecryption=True)['Parameter']['Value']
        else:
            return dbutils.secrets.get(scope="kv", key=value[1:-1])
    else:
        return value

# Validate mode against dynamic list of values.
def validate_mode(valid, mode):
    if mode not in valid:
        raise Exception("Invalid mode")
