# Return the current maximum value from the specified Delta table and column, optionally adjusted by offset.
def get_max(table, column, offset=None):
    # Return early if table does not exist.
    if not spark.catalog._jcatalog.tableExists(table):
        return None
    # Prepare query.
    column = conform_column_name(column)
    if offset is None:
        query = f"SELECT MAX({column}) FROM {table}"
    else:
        if (isinstance(offset, int) and not isinstance(offset, bool)) or (isinstance(offset, str) and match("^[0-9]+$", offset)):
            query = f"SELECT MAX({column}) - {offset} FROM {table}"
        elif isinstance(offset, str):
            query = f"SELECT MAX({column}) - INTERVAL {offset} FROM {table}"
        else:
            raise Exception("Invalid offset")
    # Execute query.
    return spark.sql(query).collect()[0][0]

# Return number of output rows from the last operation performed against the specified Delta table.
def get_num_output_rows(table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()[0][12]["numOutputRows"]

# Read from Delta table and return data frame containing current rows that have not been marked as deleted.
def read_active(table, select=None):
    ws = Window.partitionBy(col(KEY)).orderBy(col(LOADED).desc())
    df = spark.table(table).withColumn("_rn", row_number().over(ws)) \
        .where(col("_rn") == 1).drop(col("_rn")) \
        .where(col(OPERATION) != "D")
    if select is None:
        return df
    elif isinstance(select, list) or isinstance(select, str):
        return df.selectExpr(select)
    else:
        raise Exception("Invalid select")
