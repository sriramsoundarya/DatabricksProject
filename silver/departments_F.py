# Databricks notebook source
from pyspark.sql.functions import col,lit,concat

# COMMAND ----------

hosiptal1=spark.read.format("parquet").load("/mnt/bronze/hosa/departments")
hosiptal2=spark.read.format("parquet").load("/mnt/bronze/hosb/departments")

# COMMAND ----------

df_merged=hosiptal1.unionByName(hosiptal2)

# COMMAND ----------

df_merged=df_merged.withColumn("SRC_DEPTID",col("DeptID")).withColumn("Dept_id",concat(col("DeptID"),lit("_"),lit("dbname")))

# COMMAND ----------

df_merged.createOrReplaceTempView("departments")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists silver.departments(
# MAGIC DeptID string,
# MAGIC Name string,
# MAGIC datasource string,
# MAGIC SRC_DEPTID string,
# MAGIC is_quarantined boolean
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table silver.departments

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into silver.departments
# MAGIC select DeptID,Name,dbname as datasource,SRC_DEPTID,
# MAGIC case when DeptID is null or Name is null  then true else false end 
# MAGIC as is_quarantined 
# MAGIC  from departments

# COMMAND ----------


