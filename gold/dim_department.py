# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists gold

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists gold.dim_departments(
# MAGIC    Dept_Id string,
# MAGIC  SRC_Dept_Id string,
# MAGIC  Name string,
# MAGIC  datasource string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table gold.dim_departments

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into gold.dim_departments
# MAGIC select 
# MAGIC DeptID,
# MAGIC SRC_DEPTID,
# MAGIC Name,
# MAGIC dbname as datasource
# MAGIC from silver.departments where is_quarantined=false

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.dim_departments

# COMMAND ----------


