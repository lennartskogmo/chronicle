# Databricks notebook source
from chronicle import ObjectLoader

loader = ObjectLoader(concurrency = dbutils.widgets.get("concurrency"), tags = dbutils.widgets.get("tags"))

# COMMAND ----------
loader.run()

# COMMAND ----------
loader.print_errors()
