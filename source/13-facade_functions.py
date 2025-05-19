# Perform full load from source to target.
def load_full(
        reader, source, target, key, mode="insert_update_delete",
        exclude=None, ignore=None, hash=None, drop=None, where=None, parallel_number=None, parallel_column=None
    ):
    validate_mode(mode=mode, valid=["insert_update", "insert_update_delete"])
    writer = DeltaBatchWriter(mode=mode, table=target, key=key, ignore=ignore, hash=hash, drop=drop)
    df = reader.read(table=source, exclude=exclude, where=where, parallel_number=parallel_number, parallel_column=parallel_column)
    return writer.write(df)

# Perform incremental load from source to target.
def load_incremental(
        reader, source, target, key, bookmark_column, bookmark_offset=None, mode="insert_update",
        exclude=None, ignore=None, hash=None, drop=None, where=None, parallel_number=None, parallel_column=None
    ):
    validate_mode(mode=mode, valid=["insert_update"])
    writer = DeltaBatchWriter(mode=mode, table=target, key=key, ignore=ignore, hash=hash, drop=drop)
    max = get_max(table=target, column=bookmark_column, offset=bookmark_offset)
    df = reader.read_greater_than(table=source, column=bookmark_column, value=max, exclude=exclude, where=where, parallel_number=parallel_number, parallel_column=parallel_column)
    return writer.write(df)
