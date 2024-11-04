# Databricks notebook source
from pyspark.testing import assertDataFrameEqual
from chronicle import *
chronicle.EXTERNAL = None

# COMMAND ----------

# Test that only one row per key gets written even if source data frame contains identical duplicate rows.
def test__DeltaBatchWriter__insert_update__with_identical_duplicates(table="__test.DeltaBatchWriter__insert_update__with_identical_duplicates"):
    # Prepare.
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    writer = DeltaBatchWriter(mode="insert_update", table=table, key="id")

    # Test insert with duplicate "Apple" rows.
    insert_source_df = spark.createDataFrame([
        (1, "Apple",  1),
        (1, "Apple",  1),
        (1, "Apple",  1),
        (2, "Banana", 1),
        (3, "Orange", 1)
    ]).toDF("id", "fruit", "version")
    insert_expected_df = spark.createDataFrame([
        (1, "Apple",  1, "I"),
        (2, "Banana", 1, "I"),
        (3, "Orange", 1, "I")
    ]).toDF("id", "fruit", "version", OPERATION)
    writer.write(insert_source_df)
    insert_actual_df = spark.sql(f"SELECT id, fruit, version, {OPERATION} FROM {table}")
    assertDataFrameEqual(insert_expected_df, insert_actual_df)

    # Test update with duplicate "Apple" and "Orange" rows.
    update_source_df = spark.createDataFrame([
        (1, "Apple",  2),
        (1, "Apple",  2),
        (1, "Apple",  2),
        (2, "Banana", 1),
        (3, "Orange", 2),
        (3, "Orange", 2)
    ]).toDF("id", "fruit", "version")
    update_expected_df = spark.createDataFrame([
        (1, "Apple",  1, "I"),
        (1, "Apple",  2, "U"),
        (2, "Banana", 1, "I"),
        (3, "Orange", 1, "I"),
        (3, "Orange", 2, "U")
    ]).toDF("id", "fruit", "version", OPERATION)
    writer.write(update_source_df)
    update_actual_df = spark.sql(f"SELECT id, fruit, version, {OPERATION} FROM {table}")
    assertDataFrameEqual(update_expected_df, update_actual_df)

    # Test resulting active rows.
    final_expected_df = spark.createDataFrame([
        (1, "Apple",  2),
        (2, "Banana", 1),
        (3, "Orange", 2)
    ]).toDF("id", "fruit", "version")
    final_actual_df = read_active(table, ["id", "fruit", "version"])
    assertDataFrameEqual(final_expected_df, final_actual_df)

# COMMAND ----------

# Test that only one row per key gets written even if source data frame contains nonidentical duplicate rows.
def test__DeltaBatchWriter__insert_update__with_nonidentical_duplicates(table="__test.DeltaBatchWriter__insert_update__with_nonidentical_duplicates"):
    # Prepare.
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    writer = DeltaBatchWriter(mode="insert_update", table=table, key="id")

    # Test insert with duplicate "Apple" rows.
    insert_source_df = spark.createDataFrame([
        (1, "Apple",  1, "a"),
        (1, "Apple",  1, "b"),
        (1, "Apple",  1, "c"),
        (2, "Banana", 1, "a"),
        (3, "Orange", 1, "a")
    ]).toDF("id", "fruit", "version", "variation")
    insert_expected_df = spark.createDataFrame([
        (1, "Apple",  1, "I"),
        (2, "Banana", 1, "I"),
        (3, "Orange", 1, "I")
    ]).toDF("id", "fruit", "version", OPERATION)
    writer.write(insert_source_df)
    insert_actual_df = spark.sql(f"SELECT id, fruit, version, {OPERATION} FROM {table}")
    assertDataFrameEqual(insert_expected_df, insert_actual_df)

    # Test update with duplicate "Apple" and "Orange" rows.
    update_source_df = spark.createDataFrame([
        (1, "Apple",  2, "c"),
        (1, "Apple",  2, "b"),
        (1, "Apple",  2, "a"),
        (2, "Banana", 1, "a"),
        (3, "Orange", 2, "a"),
        (3, "Orange", 2, "b")
    ]).toDF("id", "fruit", "version", "variation")
    update_expected_df = spark.createDataFrame([
        (1, "Apple",  1, "I"),
        (1, "Apple",  2, "U"),
        (2, "Banana", 1, "I"),
        (3, "Orange", 1, "I"),
        (3, "Orange", 2, "U")
    ]).toDF("id", "fruit", "version", OPERATION)
    writer.write(update_source_df)
    update_actual_df = spark.sql(f"SELECT id, fruit, version, {OPERATION} FROM {table}")
    assertDataFrameEqual(update_expected_df, update_actual_df)

    # Test resulting active rows.
    final_expected_df = spark.createDataFrame([
        (1, "Apple",  2),
        (2, "Banana", 1),
        (3, "Orange", 2)
    ]).toDF("id", "fruit", "version")
    final_actual_df = read_active(table, ["id", "fruit", "version"])
    assertDataFrameEqual(final_expected_df, final_actual_df)

# COMMAND ----------

# Test that only one row per key gets written even if source data frame contains identical duplicate rows.
def test__DeltaBatchWriter__insert_update_delete__with_identical_duplicates(table="__test.DeltaBatchWriter__insert_update_delete__with_identical_duplicates"):
    # Prepare.
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    writer = DeltaBatchWriter(mode="insert_update_delete", table=table, key="id")

    # Test insert with duplicate "Apple" rows.
    insert_source_df = spark.createDataFrame([
        (1, "Apple",  1),
        (1, "Apple",  1),
        (1, "Apple",  1),
        (2, "Banana", 1),
        (3, "Orange", 1)
    ]).toDF("id", "fruit", "version")
    insert_expected_df = spark.createDataFrame([
        (1, "Apple",  1, "I"),
        (2, "Banana", 1, "I"),
        (3, "Orange", 1, "I")
    ]).toDF("id", "fruit", "version", OPERATION)
    writer.write(insert_source_df)
    insert_actual_df = spark.sql(f"SELECT id, fruit, version, {OPERATION} FROM {table}")
    assertDataFrameEqual(insert_expected_df, insert_actual_df)

    # Test update with duplicate "Apple" and "Orange" rows.
    update_source_df = spark.createDataFrame([
        (1, "Apple",  2),
        (1, "Apple",  2),
        (1, "Apple",  2),
        (2, "Banana", 1),
        (3, "Orange", 1),
        (3, "Orange", 1)
    ]).toDF("id", "fruit", "version")
    update_expected_df = spark.createDataFrame([
        (1, "Apple",  1, "I"),
        (1, "Apple",  2, "U"),
        (2, "Banana", 1, "I"),
        (3, "Orange", 1, "I")
    ]).toDF("id", "fruit", "version", OPERATION)
    writer.write(update_source_df)
    update_actual_df = spark.sql(f"SELECT id, fruit, version, {OPERATION} FROM {table}")
    assertDataFrameEqual(update_expected_df, update_actual_df)

    # Test delete with duplicate "Apple" and "Banana" rows.
    delete_source_df = spark.createDataFrame([
        (1, "Apple",  3),
        (1, "Apple",  3),
        (1, "Apple",  3),
        (2, "Banana", 2),
        (2, "Banana", 2)
    ]).toDF("id", "fruit", "version")
    delete_expected_df = spark.createDataFrame([
        (1, "Apple",  1, "I"),
        (1, "Apple",  2, "U"),
        (1, "Apple",  3, "U"),
        (2, "Banana", 1, "I"),
        (2, "Banana", 2, "U"),
        (3, "Orange", 1, "I"),
        (3, "Orange", 1, "D")
    ]).toDF("id", "fruit", "version", OPERATION)
    writer.write(delete_source_df)
    delete_actual_df = spark.sql(f"SELECT id, fruit, version, {OPERATION} FROM {table}")
    assertDataFrameEqual(delete_expected_df, delete_actual_df)

    # Test resulting active rows.
    final_expected_df = spark.createDataFrame([
        (1, "Apple",  3),
        (2, "Banana", 2)
    ]).toDF("id", "fruit", "version")
    final_actual_df = read_active(table, ["id", "fruit", "version"])
    assertDataFrameEqual(final_expected_df, final_actual_df)

# COMMAND ----------

# Test that only one row per key gets written even if source data frame contains nonidentical duplicate rows.
def test__DeltaBatchWriter__insert_update_delete__with_nonidentical_duplicates(table="__test.DeltaBatchWriter__insert_update_delete__with_nonidentical_duplicates"):
    # Prepare.
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    writer = DeltaBatchWriter(mode="insert_update_delete", table=table, key="id")

    # Test insert with duplicate "Apple" rows.
    insert_source_df = spark.createDataFrame([
        (1, "Apple",  1, "a"),
        (1, "Apple",  1, "b"),
        (1, "Apple",  1, "c"),
        (2, "Banana", 1, "a"),
        (3, "Orange", 1, "a")
    ]).toDF("id", "fruit", "version", "variation")
    insert_expected_df = spark.createDataFrame([
        (1, "Apple",  1, "I"),
        (2, "Banana", 1, "I"),
        (3, "Orange", 1, "I")
    ]).toDF("id", "fruit", "version", OPERATION)
    writer.write(insert_source_df)
    insert_actual_df = spark.sql(f"SELECT id, fruit, version, {OPERATION} FROM {table}")
    assertDataFrameEqual(insert_expected_df, insert_actual_df)

    # Test update with duplicate "Apple" and "Orange" rows.
    update_source_df = spark.createDataFrame([
        (1, "Apple",  2, "c"),
        (1, "Apple",  2, "b"),
        (1, "Apple",  2, "a"),
        (2, "Banana", 1, "a"),
        (3, "Orange", 1, "a"),
        (3, "Orange", 1, "a")
    ]).toDF("id", "fruit", "version", "variation")
    update_expected_df = spark.createDataFrame([
        (1, "Apple",  1, "I"),
        (1, "Apple",  2, "U"),
        (2, "Banana", 1, "I"),
        (3, "Orange", 1, "I")
    ]).toDF("id", "fruit", "version", OPERATION)
    writer.write(update_source_df)
    update_actual_df = spark.sql(f"SELECT id, fruit, version, {OPERATION} FROM {table}")
    assertDataFrameEqual(update_expected_df, update_actual_df)

    # Test delete with duplicate "Apple" and "Banana" rows.
    delete_source_df = spark.createDataFrame([
        (1, "Apple",  3, "a"),
        (1, "Apple",  3, "b"),
        (1, "Apple",  3, "c"),
        (2, "Banana", 2, "a"),
        (2, "Banana", 2, "b")
    ]).toDF("id", "fruit", "version", "variation")
    delete_expected_df = spark.createDataFrame([
        (1, "Apple",  1, "I"),
        (1, "Apple",  2, "U"),
        (1, "Apple",  3, "U"),
        (2, "Banana", 1, "I"),
        (2, "Banana", 2, "U"),
        (3, "Orange", 1, "I"),
        (3, "Orange", 1, "D")
    ]).toDF("id", "fruit", "version", OPERATION)
    writer.write(delete_source_df)
    delete_actual_df = spark.sql(f"SELECT id, fruit, version, {OPERATION} FROM {table}")
    assertDataFrameEqual(delete_expected_df, delete_actual_df)

    # Test resulting active rows.
    final_expected_df = spark.createDataFrame([
        (1, "Apple",  3),
        (2, "Banana", 2)
    ]).toDF("id", "fruit", "version")
    final_actual_df = read_active(table, ["id", "fruit", "version"])
    assertDataFrameEqual(final_expected_df, final_actual_df)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS __test")
test__DeltaBatchWriter__insert_update__with_identical_duplicates()
test__DeltaBatchWriter__insert_update__with_nonidentical_duplicates()
test__DeltaBatchWriter__insert_update_delete__with_identical_duplicates()
test__DeltaBatchWriter__insert_update_delete__with_nonidentical_duplicates()
