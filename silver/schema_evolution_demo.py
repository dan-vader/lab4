# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Schema enforcement, evolution and column mapping
# MAGIC Three short demos on top of the turbine_registry_silver table:
# MAGIC enforcement (write rejected), evolution (mergeSchema), column mapping (safe rename/drop).

# COMMAND ----------

dbutils.widgets.text("silver_schema", "")
silver_schema = dbutils.widgets.get("silver_schema")
spark.sql("USE CATALOG dbr_dev_ua5816bd")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Schema enforcement
# MAGIC Adding a new column without mergeSchema should fail the write.

# COMMAND ----------

from pyspark.sql.functions import lit

extra_col_df = (spark.table(f"{silver_schema}.turbine_registry_silver")
    .limit(1)
    .withColumn("manufacturer_country", lit("UA")))

extra_col_df.write.mode("append").saveAsTable(f"{silver_schema}.turbine_registry_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schema evolution
# MAGIC The same write succeeds once mergeSchema is enabled, and the new column is added.

# COMMAND ----------

(extra_col_df.write
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{silver_schema}.turbine_registry_silver"))

# COMMAND ----------

spark.table(f"{silver_schema}.turbine_registry_silver").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Column mapping
# MAGIC Enable name-based column mapping so columns can be renamed or dropped
# MAGIC without rewriting the underlying files.

# COMMAND ----------

spark.sql(f"""
ALTER TABLE {silver_schema}.turbine_registry_silver
SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')
""")

# COMMAND ----------

spark.sql(f"""
ALTER TABLE {silver_schema}.turbine_registry_silver
RENAME COLUMN manufacturer_country TO country_code
""")

# COMMAND ----------

spark.table(f"{silver_schema}.turbine_registry_silver").printSchema()