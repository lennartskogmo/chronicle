# Delta table batch writer.
class DeltaBatchWriter:
    
    # Capture inserted records from full or partial source data frame.
    #def __insert(self, df):
    #    df = self.__add_checksum_and_key_columns(df).alias("s") \
    #        .join(read_active(self.table, "_key").alias("a"), "_key", "anti") \
    #        .withColumn("_operation", lit("I")) \
    #        .withColumn("_timestamp", current_timestamp()) \
    #        .select("_operation", "_timestamp", "s.*")
    #    df.write.format("delta").mode("append").saveAsTable(self.table)

    # Capture inserted and updated records from full or partial source data frame.
    #def __insert_update(self, df):
    #    df = self.__add_checksum_and_key_columns(df).alias("s") \
    #        .join(read_active(self.table, ["_key", "_checksum"]).alias("a"), "_key", "left") \
    #        .withColumn("_operation", expr("CASE WHEN a._key IS NULL THEN 'I' ELSE 'U' END")) \
    #        .withColumn("_timestamp", current_timestamp()) \
    #        .where("(a._key IS NULL) OR (a._checksum != s._checksum)") \
    #        .select("_operation", "_timestamp", "s.*")
    #    df.write.format("delta").mode("append").saveAsTable(self.table)

    # Capture deleted records from full or key only source data frame.
    #def __delete(self, df):
    #    df = read_active(self.table, "* EXCEPT (_operation, _timestamp)").alias("a") \
    #        .join(add_key_column(df, self.key).alias("s"), "_key", "anti") \
    #        .withColumn("_operation", lit("D")) \
    #        .withColumn("_timestamp", current_timestamp()) \
    #        .select("_operation", "_timestamp", "a.*")
    #    df.write.format("delta").mode("append").saveAsTable(self.table)
