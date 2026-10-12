# Databricks notebook source
   dbutils.widgets.text("silver_schema", "")
   silver_schema = dbutils.widgets.get("silver_schema")

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
   USING DELTA
   """)