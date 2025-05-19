# Databricks notebook source
# MAGIC %run ./chronicle.py

# COMMAND ----------
loader = ObjectLoader(concurrency = dbutils.widgets.get("concurrency"), tags = dbutils.widgets.get("tags"))

# COMMAND ----------
loader.run()

# COMMAND ----------
loader.print_errors()
