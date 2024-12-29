# Databricks notebook source
from pyspark.sql.functions import col,when

# COMMAND ----------

providers_df1=spark.read.parquet("/mnt/bronze/hosa/providers")
providers_df2=spark.read.parquet("/mnt/bronze/hosb/providers")

# COMMAND ----------

df_merged = providers_df1.unionByName(providers_df2)
display(df_merged)

df_merged.createOrReplaceTempView("providers")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC create schema if not exists silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists silver.providers(
# MAGIC   ProviderID string,
# MAGIC   FirstName string,
# MAGIC   LastName string,
# MAGIC   Specialization string,
# MAGIC   DeptID string,
# MAGIC   NPI long,
# MAGIC   datasource string,
# MAGIC   is_quarantined boolean
# MAGIC ) using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table silver.providers

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into silver.providers(
# MAGIC   select ProviderID,
# MAGIC   FirstName,
# MAGIC   LastName,
# MAGIC   Specialization,
# MAGIC   DeptID,
# MAGIC   cast(NPI as INT),
# MAGIC   dbname as datasource,
# MAGIC   case 
# MAGIC      when DeptID is null or ProviderID is null then True else False end as 
# MAGIC   is_quarantined
# MAGIC    from providers
# MAGIC )

# COMMAND ----------


