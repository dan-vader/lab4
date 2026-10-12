# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver layer - turbine registry SCD2
# MAGIC Deduplicates the latest bronze snapshot per turbine and merges it into the silver
# MAGIC table, keeping full history via valid_from / valid_to / is_current.

# COMMAND ----------

dbutils.widgets.text("bronze_schema", "")
dbutils.widgets.text("silver_schema", "")
dbutils.widgets.text("catalog", "")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

   from datetime import datetime

   run_ts = datetime.utcnow()
   run_ts_str = run_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_schema}.turbine_registry_silver (
    turbine_id STRING,
    model STRING,
    rated_power_kw INT,
    location STRING,
    status STRING,
    row_hash STRING,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN
)
""")

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import sha2, concat_ws, col, row_number, desc

bronze_df = spark.table(f"{bronze_schema}.turbine_registry_bronze")

hashed_df = bronze_df.withColumn("row_hash", sha2(concat_ws("||",
    col("model"), col("rated_power_kw").cast("string"),
    col("location"), col("status")), 256))

window_spec = Window.partitionBy("turbine_id").orderBy(desc("source_ts"))

latest_df = (hashed_df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .select("turbine_id", "model", "rated_power_kw", "location", "status", "row_hash"))

# COMMAND ----------

latest_df.createOrReplaceTempView("staged_latest")

# COMMAND ----------

spark.sql(f"""
MERGE INTO {silver_schema}.turbine_registry_silver AS target
USING staged_latest AS source
ON target.turbine_id = source.turbine_id AND target.is_current = true
WHEN MATCHED AND target.row_hash != source.row_hash THEN
  UPDATE SET target.valid_to = TIMESTAMP'{run_ts_str}', target.is_current = false
""")

# COMMAND ----------

spark.sql(f"""
INSERT INTO {silver_schema}.turbine_registry_silver
    (turbine_id, model, rated_power_kw, location, status, row_hash, valid_from, valid_to, is_current)
SELECT
    s.turbine_id, s.model, s.rated_power_kw, s.location, s.status, s.row_hash,
    TIMESTAMP'{run_ts_str}' AS valid_from,
    CAST(NULL AS TIMESTAMP) AS valid_to,
    true AS is_current
FROM staged_latest s
LEFT JOIN {silver_schema}.turbine_registry_silver t
    ON s.turbine_id = t.turbine_id AND t.is_current = true AND t.row_hash = s.row_hash
WHERE t.turbine_id IS NULL
""")

# COMMAND ----------

display(spark.table(f"{silver_schema}.turbine_registry_silver").orderBy("turbine_id", "valid_from"))