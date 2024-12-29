# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_icd (
# MAGIC     icd_code STRING,
# MAGIC     icd_code_type STRING,
# MAGIC     code_description STRING,
# MAGIC     refreshed_at TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table gold.dim_icd

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into gold.dim_icd
# MAGIC select
# MAGIC icd_code,
# MAGIC icd_code_type,
# MAGIC code_description,
# MAGIC current_timestamp as refreshed_at
# MAGIC from silver.icd_codes where is_current_flag=true

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from gold.dim_icd

# COMMAND ----------


