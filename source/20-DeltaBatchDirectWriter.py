# Delta table batch direct writer.
class DeltaBatchDirectWriter:

    # Initialize writer.
    def __init__(self, mode, table, hash=None, drop=None):
        validate_mode(mode=mode, valid=["append", "overwrite"])
        self.drop   = drop
        self.hash   = hash
        self.mode   = mode
        self.table  = table

    # Create Delta table with added metadata columns ANONYMIZED and LOADED.
    def __create_table(self, df):
        df = df.select([current_timestamp().alias(LOADED), "*"])
        df = df.select([current_timestamp().alias(ANONYMIZED), "*"])
        df = df.filter("1=0")
        dw = df.write.format("delta") \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .option("delta.autoOptimize.autoCompact", "true")
        if EXTERNAL_PATH is not None:
            dw = dw.option("path", EXTERNAL_PATH + self.table.replace(".", "/"))
        if SNOWFLAKE_COMPATIBILITY is not None:
            dw = dw.option("delta.checkpointPolicy", "classic")
            dw = dw.option("delta.enableDeletionVectors", "false")
            dw = dw.option("delta.enableRowTracking", "false")
        dw.saveAsTable(self.table)

    # Prepare data frame and Delta table for writing.
    def __prepare(self, df):
        # Perform column transformations.
        df = conform_column_names(df)                      # 1 Conform column names
        if self.hash: df = add_hash_columns(df, self.hash) # 2 Add hash columns
        if self.drop: df = drop_columns(df, self.drop)     # 3 Drop columns
        # Create table.
        if not spark.catalog._jcatalog.tableExists(self.table):
            self.__create_table(df)
        return df

    # Write data frame to Delta table.
    def write(self, df):
        df = self.__prepare(df)
        df = df.select([current_timestamp().alias(LOADED), "*"])
        df.write.format("delta").mode(self.mode).option("mergeSchema", "true").saveAsTable(self.table)
        return get_num_output_rows(self.table)
