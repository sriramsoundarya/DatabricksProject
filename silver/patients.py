# Databricks notebook source
from pyspark.sql.functions import concat

# COMMAND ----------

patients_df1=spark.read.parquet("/mnt/bronze/hosa/patients")
patients_df1.createOrReplaceTempView("patients1")

# COMMAND ----------

patients_df2=spark.read.parquet("/mnt/bronze/hosb/patients")
patients_df2.createOrReplaceTempView("patients2")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create Table if not exists silver.patients(
# MAGIC      Patient_Key STRING,
# MAGIC      SRC_PatientID STRING,
# MAGIC      FirstName STRING,
# MAGIC      LastName STRING,
# MAGIC      MiddleName STRING,
# MAGIC      SSN STRING,
# MAGIC      PhoneNumber STRING,
# MAGIC      Gender STRING,
# MAGIC      DOB DATE,
# MAGIC      Address STRING,
# MAGIC      SRC_ModifiedDate TIMESTAMP,
# MAGIC      datasource STRING,
# MAGIC      is_quarantined BOOLEAN,
# MAGIC      inserted_date TIMESTAMP,
# MAGIC      modified_date TIMESTAMP,
# MAGIC      is_current BOOLEAN
# MAGIC ) using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW cdm_patients AS
# MAGIC select concat(SRC_PatientID,'-', datasource) as Patient_Key,* FROM (
# MAGIC   SELECT 
# MAGIC   PatientID as SRC_PatientID,
# MAGIC   FirstName,
# MAGIC   LastName,
# MAGIC   MiddleName,
# MAGIC   SSN,
# MAGIC   PhoneNumber,
# MAGIC   Gender,
# MAGIC   DOB,
# MAGIC   Address,
# MAGIC   ModifiedDate AS SRC_ModifiedDate,
# MAGIC   datasource
# MAGIC   from patients1
# MAGIC   union all
# MAGIC   SELECT
# MAGIC    PatientID AS SRC_PatientID,
# MAGIC    FirstName,
# MAGIC    LastName,
# MAGIC    MiddleName,
# MAGIC    SSN,
# MAGIC    PhoneNumber,
# MAGIC    Gender,
# MAGIC    DOB,
# MAGIC    Address,
# MAGIC    ModifiedDate as SRC_ModifiedDate,
# MAGIC    datasource
# MAGIC     FROM patients2
# MAGIC )
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace Temp view qualitychecks_patients as
# MAGIC select *,
# MAGIC  CASE 
# MAGIC     WHEN SRC_PatientID IS NULL OR dob IS NULL OR firstname IS NULL or 
# MAGIC     lower(firstname)='null' THEN TRUE
# MAGIC          ELSE FALSE
# MAGIC      END AS is_quarantined from cdm_patients

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into silver.patients as target
# MAGIC using qualitychecks_patients as source
# MAGIC on target.Patient_Key=source.Patient_Key
# MAGIC and target.is_current=true
# MAGIC when matched 
# MAGIC and(
# MAGIC    target.SRC_PatientID <> source.SRC_PatientID OR
# MAGIC      target.FirstName <> source.FirstName OR
# MAGIC      target.LastName <> source.LastName OR
# MAGIC      target.MiddleName <> source.MiddleName OR
# MAGIC      target.SSN <> source.SSN OR
# MAGIC      target.PhoneNumber <> source.PhoneNumber OR
# MAGIC      target.Gender <> source.Gender OR
# MAGIC      target.DOB <> source.DOB OR
# MAGIC      target.Address <> source.Address OR
# MAGIC      target.SRC_ModifiedDate <> source.SRC_ModifiedDate OR
# MAGIC      target.datasource <> source.datasource OR
# MAGIC      target.is_quarantined <> source.is_quarantined
# MAGIC )
# MAGIC then update 
# MAGIC set target.is_current=False,
# MAGIC target.modified_date=current_timestamp()
# MAGIC when not matched
# MAGIC then INSERT (
# MAGIC      Patient_Key,
# MAGIC      SRC_PatientID,
# MAGIC      FirstName,
# MAGIC      LastName,
# MAGIC      MiddleName,
# MAGIC      SSN,
# MAGIC      PhoneNumber,
# MAGIC      Gender,
# MAGIC      DOB,
# MAGIC      Address,
# MAGIC      SRC_ModifiedDate,
# MAGIC      datasource,
# MAGIC      is_quarantined,
# MAGIC      inserted_date,
# MAGIC      modified_date,
# MAGIC      is_current
# MAGIC  )
# MAGIC  VALUES (
# MAGIC      source.Patient_Key,
# MAGIC      source.SRC_PatientID,
# MAGIC      source.FirstName,
# MAGIC      source.LastName,
# MAGIC      source.MiddleName,
# MAGIC      source.SSN,
# MAGIC      source.PhoneNumber,
# MAGIC      source.Gender,
# MAGIC      source.DOB,
# MAGIC      source.Address,
# MAGIC      source.SRC_ModifiedDate,
# MAGIC      source.datasource,
# MAGIC      source.is_quarantined,
# MAGIC      current_timestamp(),
# MAGIC      current_timestamp(),
# MAGIC      true
# MAGIC  );

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into silver.patients
# MAGIC as target
# MAGIC using qualitychecks_patients as source
# MAGIC ON target.Patient_Key=source.Patient_Key
# MAGIC AND target.is_current=true
# MAGIC when not matched
# MAGIC THEN INSERT (
# MAGIC      Patient_Key,
# MAGIC      SRC_PatientID,
# MAGIC      FirstName,
# MAGIC      LastName,
# MAGIC      MiddleName,
# MAGIC      SSN,
# MAGIC      PhoneNumber,
# MAGIC      Gender,
# MAGIC      DOB,
# MAGIC      Address,
# MAGIC      SRC_ModifiedDate,
# MAGIC      datasource,
# MAGIC      is_quarantined,
# MAGIC      inserted_date,
# MAGIC      modified_date,
# MAGIC      is_current
# MAGIC  )
# MAGIC  VALUES (
# MAGIC      source.Patient_Key,
# MAGIC      source.SRC_PatientID,
# MAGIC      source.FirstName,
# MAGIC      source.LastName,
# MAGIC      source.MiddleName,
# MAGIC      source.SSN,
# MAGIC      source.PhoneNumber,
# MAGIC      source.Gender,
# MAGIC      source.DOB,
# MAGIC      source.Address,
# MAGIC      source.SRC_ModifiedDate,
# MAGIC      source.datasource,
# MAGIC      source.is_quarantined,
# MAGIC      current_timestamp(), -- Set inserted_date to current timestamp
# MAGIC      current_timestamp(), -- Set modified_date to current timestamp
# MAGIC      true -- Mark as current
# MAGIC  );

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct is_current from silver.patients

# COMMAND ----------

# MAGIC  %sql
# MAGIC  select count(*),Patient_Key from silver.patients
# MAGIC  group by patient_key
# MAGIC  order by 1 desc
