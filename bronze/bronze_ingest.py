# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze layer – turbine registry ingest
# MAGIC Reads the raw JSON snapshots from the volume and appends them to the bronze table, tagging each row with ingestion time and source file.

# COMMAND ----------


dbutils.widgets.text("schema", "")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

spark.sql("USE CATALOG dbr_dev_ua5816bd")

# COMMAND ----------

raw_df = spark.read.json(f"/Volumes/dbr_dev_ua5816bd/{schema}/raw")

# COMMAND ----------

raw_df.printSchema()
display(raw_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

bronze_df = (raw_df
    .withColumn("ingestion_ts", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path")))

# COMMAND ----------

(bronze_df.write
    .mode("append")
    .saveAsTable(f"{schema}.turbine_registry_bronze"))

# COMMAND ----------

display(spark.table(f"{schema}.turbine_registry_bronze").orderBy("ingestion_ts"))