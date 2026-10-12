# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Maintenance - optimize and vacuum
# MAGIC Recurring table maintenance for the silver turbine registry table.
# MAGIC Run as part of the scheduled job, after each silver run.

# COMMAND ----------

dbutils.widgets.text("silver_schema", "")
dbutils.widgets.text("catalog", "")
silver_schema = dbutils.widgets.get("silver_schema")
catalog = dbutils.widgets.get("catalog")

spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

display(spark.sql(f"OPTIMIZE {silver_schema}.turbine_registry_silver"))

# COMMAND ----------

spark.sql(f"VACUUM {silver_schema}.turbine_registry_silver RETAIN 168 HOURS")

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {silver_schema}.turbine_registry_silver").limit(10))