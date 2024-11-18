#
def load_object(object, connection=None, connection_with_secrets=None):
    # Prepare object configuration.
    if isinstance(object, str):
        object = get_object(object)
    if not isinstance(object, dict):
        raise Exception("Invalid object")
    # Prepare connection configuration.
    if connection is None:
        connection = get_connection(object["ConnectionName"])
    if not isinstance(connection, dict):
        raise Exception("Invalid connection")
    # Prepare connection configuration with secrets.
    if connection_with_secrets is None:
        connection_with_secrets = get_connection_with_secrets(connection)
    if not isinstance(connection_with_secrets, dict):
        raise Exception("Invalid connection with secrets")
    # Map connection configuration with secrets to reader constructor arguments.
    reader_arguments = map_reader_arguments(connection_with_secrets)
    # Map object configuration to function arguments.
    function_arguments = map_function_arguments(object)
    # Instantiate reader.
    reader = globals()[connection_with_secrets["Reader"]]
    function_arguments["reader"] = reader(**reader_arguments)
    # Invoke function.
    function = globals()[object["Function"]]
    return function(**function_arguments)

# Perform full load.
def load_full(
        reader, source, target, key, mode="insert_update_delete",
        exclude=None, ignore=None, hash=None, drop=None, where=None, parallel_number=None, parallel_column=None
    ):
    validate_mode(mode=mode, valid=["insert_update", "insert_update_delete"])
    writer = DeltaBatchWriter(mode=mode, table=target, key=key, ignore=ignore, hash=hash, drop=drop)
    df = reader.read(table=source, exclude=exclude, where=where, parallel_number=parallel_number, parallel_column=parallel_column)
    return writer.write(df)

# Perform incremental load.
def load_incremental(
        reader, source, target, key, bookmark_column, bookmark_offset=None, mode="insert_update",
        exclude=None, ignore=None, hash=None, drop=None, where=None, parallel_number=None, parallel_column=None
    ):
    validate_mode(mode=mode, valid=["insert_update"])
    writer = DeltaBatchWriter(mode=mode, table=target, key=key, ignore=ignore, hash=hash, drop=drop)
    max = get_max(table=target, column=bookmark_column, offset=bookmark_offset)
    df = reader.read_greater_than(table=source, column=bookmark_column, value=max, exclude=exclude, where=where, parallel_number=parallel_number, parallel_column=parallel_column)
    return writer.write(df)
