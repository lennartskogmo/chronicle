# Databricks notebook source
from os import listdir
from yaml import safe_load
from chronicle import CHRONICLE, CONNECTION, OBJECT

def prepare_database():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CHRONICLE}")
    spark.sql(f"DROP TABLE IF EXISTS {CONNECTION}")
    spark.sql(f"DROP TABLE IF EXISTS {OBJECT}")

def parse_directory(path):
    items = []
    for file in listdir(path):
        if file.lower().endswith(".yml"):
            file = open(path + file, "r")
            items += safe_load(file)
            file.close()
    return items

def write_connections():
    connections = parse_directory("/Workspace/Shared/Chronicle/Configuration/Connections/")
    connections = spark.createDataFrame(connections)
    connections.repartition(1).write.format("delta").saveAsTable(CONNECTION)

def write_objects():
    objects = parse_directory("/Workspace/Shared/Chronicle/Configuration/Objects/")
    objects = spark.createDataFrame(objects)
    objects.repartition(1).write.format("delta").saveAsTable(OBJECT)

# COMMAND ----------

prepare_database()

# COMMAND ----------

write_connections()

# COMMAND ----------

write_objects()
