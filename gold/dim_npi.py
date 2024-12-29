# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists gold

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists gold.dim_npi(
# MAGIC npi_id STRING,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   position STRING,
# MAGIC   organisation_name STRING,
# MAGIC   last_updated STRING,
# MAGIC   refreshed_at TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table gold.dim_npi

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC insert into
# MAGIC   gold.dim_npi
# MAGIC select
# MAGIC   npi_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   position,
# MAGIC   organisation_name,
# MAGIC   last_updated,
# MAGIC   current_timestamp()
# MAGIC from
# MAGIC   silver.npi_extract
# MAGIC   where is_current_flag = true

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.dim_npi

# COMMAND ----------


