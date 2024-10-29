# Perform append only incremental load.
#def load_appended(reader, source, target, bookmark_column, key=None, exclude=None, ignore=None, where=None, mode="a", parallel_number=None, parallel_column=None):
#    validate_mode(valid=["a", "ka"], mode=mode)
#    reader = get_reader(reader=reader)
#    writer = DeltaBatchWriter(mode=mode, table=target, key=key, ignore=ignore)
#    max = get_max(table=target, column=bookmark_column)
#    df = reader.read_greater_than(table=source, column=bookmark_column, value=max, exclude=exclude, where=where, parallel_number=parallel_number, parallel_column=parallel_column)
#    writer.write(df)
#    print(f"Result rows: {get_num_output_rows(target)}")

# Capture deleted records by comparing source and target keys.
#def load_deleted(reader, source, target, key, where=None):
#    reader = get_reader(reader=reader)
#    writer = DeltaBatchWriter(mode="d", table=target, key=key)
#    df = reader.read_key(table=source, key=key, where=where)
#    writer.write(df)
#    print(f"Result rows: {get_num_output_rows(target)}")

# Perform or continue full backfill by splitting source data into multiple batches and appending one batch at a time.
#def backfill_full(reader, source, target, backfill_column, key=None, exclude=None, ignore=None, where=None, mode="a", parallel_number=None, parallel_column=None):
#    validate_mode(valid=["a", "ka"], mode=mode)
#    reader = get_reader(reader=reader)
#    writer = DeltaBatchWriter(mode=mode, table=target, key=key, ignore=ignore)
#    max = get_max(table=target, column=backfill_column)
#    distinct = reader.get_distinct(table=source, column=backfill_column, greater_than=max, where=where)
#    print(f"Batch count: {len(distinct)}")
#    for value in distinct:
#        print(f"Batch value: {value}")
#        df = reader.read_equal_to(table=source, column=backfill_column, value=value, exclude=exclude, where=where, parallel_number=parallel_number, parallel_column=parallel_column)
#        writer.write(df)
#        print(f"Result rows: {get_num_output_rows(target)}")
