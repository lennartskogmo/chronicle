# Databricks notebook source
# MAGIC %run ./chronicle.py

# COMMAND ----------
df = spark.sql(f"SELECT DISTINCT LOWER(SPLIT(ObjectName, '\\\.')[0]) AS schema_name FROM {OBJECT} WHERE Status = 'Active'")

for row in df.rdd.collect():
    print(row['schema_name'])
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {row['schema_name']}")
