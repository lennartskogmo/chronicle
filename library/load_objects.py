# Databricks notebook source
from chronicle import ObjectLoader

loader = ObjectLoader(concurrency = dbutils.widgets.get("concurrency"), tag = dbutils.widgets.get("tag"))

# COMMAND ----------
loader.run()

# COMMAND ----------
loader.print_errors()
