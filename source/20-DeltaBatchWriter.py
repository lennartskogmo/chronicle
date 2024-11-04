# Delta table batch writer.
class DeltaBatchWriter:

    # Initialize writer.
    def __init__(self, mode, table, key, ignore=None, hash=None, drop=None):
        validate_mode(mode=mode, valid=["append", "insert_update", "insert_update_delete"])
        self.key    = key
        self.drop   = drop
        self.hash   = hash
        self.mode   = mode
        self.table  = table
        self.ignore = ignore
    
    # Create Delta table with added metadata columns ANONYMIZED, LOADED, OPERATION, CHECKSUM and KEY.
    def __create_table(self, df):
        df = df.select([lit("C").alias(OPERATION), "*"])
        df = df.select([current_timestamp().alias(LOADED), "*"])
        df = df.select([current_timestamp().alias(ANONYMIZED), "*"])
        df = df.filter("1=0")
        dw = df.write.format("delta") \
            .clusterBy(KEY, LOADED, OPERATION) \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .option("delta.autoOptimize.autoCompact", "true")
        if EXTERNAL is not None:
            dw = dw.option("path", EXTERNAL + self.table.replace(".", "/"))
        dw.saveAsTable(self.table)
    
    # Prepare data frame and Delta table for writing.
    def __prepare(self, df):
        # Perform column transformations.
        df = conform_column_names(df)                      # 1 Conform column names
        if self.hash: df = add_hash_columns(df, self.hash) # 2 Add hash columns
        df = add_key_column(df, self.key)                  # 3 Add key column
        if self.drop: df = drop_columns(df, self.drop)     # 4 Drop columns
        df = add_checksum_column(df, self.ignore)          # 5 Add checksum column
        # Remove duplicates
        # Create table.
        if not spark.catalog._jcatalog.tableExists(self.table):
            self.__create_table(df)
            return df, True
        else:
            return df, False
    
    # Write data frame to Delta table.
    def write(self, df):
        df, new = self.__prepare(df)
        # mode = "append" if new else self.mode
        mode = self.mode
        if mode == "append":
            self.__append(df)
        elif mode == "insert_update":
            self.__insert_update(df)
        elif mode == "insert_update_delete":
            self.__insert_update_delete(df)
        else:
            raise Exception("Invalid mode")
        return get_num_output_rows(self.table)
    
    # Append source data frame.
    def __append(self, df):
        df = df.select([lit("I").alias(OPERATION), "*"])
        df = df.select([current_timestamp().alias(LOADED), "*"])
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(self.table)
    
    # Capture inserted and updated records from full or partial source data frame.
    # Will only write one row per key even if source data frame contains duplicate rows.
    # Arbitrary row will be used if source data frame contains nonidentical duplicate rows.
    def __insert_update(self, df):
        ws = Window.partitionBy(col(KEY)).orderBy(col("_origin"))
        df = df.withColumn("_origin", lit(1)) \
        .unionByName(
            read_active(self.table, [f"{KEY}", f"{CHECKSUM}"]).withColumn("_origin", lit(2)),
            allowMissingColumns = True) \
        .select([
            when((col("_origin") == 1) & (lead(col(KEY)).over(ws).isNull()), "I") \
            .when((col("_origin") == 1) & (lead(col("_origin")).over(ws) == 2) & (col(CHECKSUM) != lead(col(CHECKSUM)).over(ws)), "U") \
            .alias(OPERATION),
            current_timestamp().alias(LOADED),
            "*"]) \
        .drop(col("_origin")).where(col(OPERATION).isNotNull())
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(self.table)
    
    # Capture inserted, updated and deleted records from full source data frame.
    # Will only write one row per key even if source data frame contains duplicate rows.
    # Arbitrary row will be used if source data frame contains nonidentical duplicate rows.
    def __insert_update_delete(self, df):
        ws = Window.partitionBy(col(KEY)).orderBy(col("_origin"))
        df = df.withColumn("_origin", lit(1)) \
        .unionByName(
            read_active(self.table, f"* EXCEPT ({OPERATION}, {LOADED})").withColumn("_origin", lit(2)),
            allowMissingColumns = True) \
        .select([
            when((col("_origin") == 1) & (lead(col(KEY)).over(ws).isNull()), "I") \
            .when((col("_origin") == 1) & (lead(col("_origin")).over(ws) == 2) & (col(CHECKSUM) != lead(col(CHECKSUM)).over(ws)), "U") \
            .when((col("_origin") == 2) & (lag(col(KEY)).over(ws).isNull()), "D") \
            .alias(OPERATION),
            current_timestamp().alias(LOADED),
            "*"]) \
        .drop(col("_origin")).where(col(OPERATION).isNotNull())
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(self.table)
