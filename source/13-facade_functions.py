# Perform full change load from source to target.
def load_change_full(
        reader, source, target, key, mode="insert_update_delete",
        exclude=None, ignore=None, hash=None, drop=None, where=None, parallel_number=None, parallel_column=None, transform=None
    ):
    validate_mode(mode=mode, valid=["insert_update", "insert_update_delete"])
    writer = DeltaBatchChangeWriter(mode=mode, table=target, key=key, ignore=ignore, hash=hash, drop=drop)
    df = reader.read(table=source, exclude=exclude, where=where, parallel_number=parallel_number, parallel_column=parallel_column)
    df = transform(df) if transform is not None else df
    return writer.write(df)

# Perform incremental change load from source to target.
def load_change_incremental(
        reader, source, target, key, bookmark_column, bookmark_offset=None, mode="insert_update",
        exclude=None, ignore=None, hash=None, drop=None, where=None, parallel_number=None, parallel_column=None, transform=None
    ):
    validate_mode(mode=mode, valid=["insert_update"])
    writer = DeltaBatchChangeWriter(mode=mode, table=target, key=key, ignore=ignore, hash=hash, drop=drop)
    max = get_max(table=target, column=bookmark_column, offset=bookmark_offset)
    df = reader.read_greater_than(table=source, column=bookmark_column, value=max, exclude=exclude, where=where, parallel_number=parallel_number, parallel_column=parallel_column)
    df = transform(df) if transform is not None else df
    return writer.write(df)

# Perform full direct load from source to target.
def load_direct_full(
        reader, source, target,
        exclude=None, hash=None, drop=None, where=None, parallel_number=None, parallel_column=None, transform=None
    ):
    writer = DeltaBatchDirectWriter(mode="overwrite", table=target, hash=hash, drop=drop)
    df = reader.read(table=source, exclude=exclude, where=where, parallel_number=parallel_number, parallel_column=parallel_column)
    df = transform(df) if transform is not None else df
    return writer.write(df)
