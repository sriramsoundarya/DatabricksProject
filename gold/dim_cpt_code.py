# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists gold

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists gold.dim_cpt_codes (
# MAGIC   cpt_codes string,
# MAGIC  procedure_code_category string,
# MAGIC  procedure_code_descriptions string,
# MAGIC  code_status string,
# MAGIC  refreshed_at timestamp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table gold.dim_cpt_codes

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into gold.dim_cpt_codes
# MAGIC select 
# MAGIC cpt_codes,
# MAGIC  procedure_code_category,
# MAGIC  procedure_code_descriptions,
# MAGIC  code_status,
# MAGIC  current_timestamp() as refreshed_at 
# MAGIC  from silver.cptcodes
# MAGIC  where is_current = true and is_quarantined=false

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.dim_cpt_codes

# COMMAND ----------


