-- Databricks notebook source
-- MAGIC %md
-- MAGIC #####1. Create a multinode shared mode cluster to work with DLT generated tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####3. Setup the catalog and external location

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS dev
MANAGED LOCATION 'abfss://container2@storageforad.dfs.core.windows.net/';


CREATE DATABASE IF NOT EXISTS dev.demo_db
MANAGED LOCATION 'abfss://container2@storageforad.dfs.core.windows.net/';


-- COMMAND ----------

-- DBTITLE 1,landing zone where data exists

CREATE EXTERNAL VOLUME IF NOT EXISTS dev.demo_db.landing_zone
LOCATION 'abfss://container2@storageforad.dfs.core.windows.net/';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####4. Ingest some data

-- COMMAND ----------

-- DBTITLE 1,create folders for staging
-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs("dbfs:/Volumes/dev/demo_db/landing_zone/customers")
-- MAGIC
-- MAGIC dbutils.fs.mkdirs("dbfs:/Volumes/dev/demo_db/landing_zone/invioces")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("dbfs:/Volumes/dev/demo_db/landing_zone/invioces/")

-- COMMAND ----------

-- DBTITLE 1,copy customer file
-- MAGIC %python
-- MAGIC dbutils.fs.cp("/Volumes/dev/demo_db/landing_zone/customers_1.csv", "/Volumes/dev/demo_db/landing_zone/customers")

-- COMMAND ----------

-- DBTITLE 1,copy invoices file
-- MAGIC %python
-- MAGIC dbutils.fs.cp("/Volumes/dev/demo_db/landing_zone/invoices_2022.csv" ,"/Volumes/dev/demo_db/landing_zone/invoices")
-- MAGIC
-- MAGIC dbutils.fs.cp("/Volumes/dev/demo_db/landing_zone/invoices_2021.csv" ,"/Volumes/dev/demo_db/landing_zone/invoices")

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC dbutils.fs.ls("dbfs:/Volumes/dev/demo_db/landing_zone/customers/)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####7. read the data

-- COMMAND ----------

-- DBTITLE 1,from vol read
-- MAGIC %python
-- MAGIC file_path = "/Volumes/dev/demo_db/landing_zone/customers_1.csv"
-- MAGIC
-- MAGIC df = spark.read.format("csv").option("header", "true").load(file_path)
-- MAGIC
-- MAGIC display(df)
