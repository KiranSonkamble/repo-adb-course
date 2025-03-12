-- Databricks notebook source
-- MAGIC %md
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://blog.scholarnest.com/wp-content/uploads/2023/03/scholarnest-academy-scaled.jpg" alt="ScholarNest Academy" style="width: 1400px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####1. Create your bronze layer tables ingesting from the landing zone

-- COMMAND ----------

-- DBTITLE 1,create a streaming table fro cust
-- creata a table using increamental approach
CREATE OR REFRESH STREAMING TABLE customers_raw
--load data into table
AS SELECT * , current_timestamp() as loaded_time
-- from cloud where landing zone exist
FROM CLOUD_FILES("/Volumes/dev/demo_db/landing_zone/customers", "csv",
                -- with autoloader autoincrem, schema evol, schema validation pre-confi but I'm giving extra config
                map("cloudFiles.inferColumnTypes","true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The purpose of this code is to create a streaming table that continuously ingests and processes new data files as they arrive in the specified cloud storage location. This is useful for scenarios where you need to handle real-time or near-real-time data ingestion and processing, such as updating a customer database with new records as they are added to the landing zone.

-- COMMAND ----------

-- DBTITLE 1,streaming table for invioces
CREATE OR REFRESH STREAMING TABLE invioces_raw
AS SELECT *, TIME_STAMP() AS loaded_time
FROM CLOUD_FILES("/Volumes/dev/demo_db/landing_zone/invioces",'csv',
                map("cloudFiles.inferColumnTypes","true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####2. Create your silver layer tables reading incremental data from bronze layer

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE 
customer_cleaned(
CONSTRAINT valid_customer_id EXPECT(customer_id IS NOT NULL) ON VIOLATION DROP ROW)
AS 
SELECT *
FROM STREAM(LIVE.customer_raw);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE invioces_cleaned(
  CONSTRAINT valid_invoices_and_qty EXPECT(invoice_no IS NOT NULL AND quantity>0) ON VIOLATION DROP ROW)
  PARTITIONED BY (invioce_year, country)
AS
SELECT InvoiceNo as invoice_no, StockCode as stock_code, Description as description,
        Quantity as quantity, to_date(InvoiceDate, "d-M-y H.m") as invoice_date, 
        UnitPrice as unit_price, CustomerID as customer_id, Country as country,
        year(to_date(InvoiceDate, "d-M-y H.m")) as invoice_year, 
        month(to_date(InvoiceDate, "d-M-y H.m")) as invoice_month,
        load_time
FROM STREAM(LIVE.invoices_raw)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This code ensures that the customer_cleaned table is continuously updated with data from the customers_raw streaming table, while enforcing the constraint that customer_id must not be null
-- MAGIC
-- MAGIC In DLT data validation checked using expectation constraints

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE
invioces_cleaned( 
  CONSTRAINT valid_invoice_id EXPECT(invoice_id IS NOT nuLL) ON VIOLATION DROP ROW
) 
AS SELECT * 
FROM STREAM(LIVE.invioces_raw); 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####3. Build your SCD Type 2 dimensions using CDC from silver layer

-- COMMAND ----------

-- DBTITLE 1,CREATE A TABLE & SCD2
CREATE OR REFRESH STREAMING TABLE customers;

-- PERFORM SCD TYPE-2
APPLY CHANGES INTO LIVE.customers
FROM STREAM(LIVE.customer_cleaned)
KEYS(customer_id)
-- sequentially update flag/ timestamp if duplicate records arrive
SEQUENCE BY loaded_time
STORED AS SCD TYPE 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC What SCD type2:
-- MAGIC OLd customer record closed and marked as "INACTIVE" with timstamp(), and new updated information  will get inserted into customer_table and marked as "ACTIVE" with timestamp().

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####4. Merge into your fact table using CDC from the silver layer

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE invoices 
PARTITIONED BY (invoice_year, country);

APPLY CHANGES INTO LIVE.invoices
FROM STREAM(LIVE.invioces_cleaned)
-- composite key 
KEYS(invoice_no,stock_code,invoice_date)
SEQUENCE BY loaded_time;  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####5. Materialize your gold layer summary using silver layer fact

-- COMMAND ----------

-- materialize table
CREATE OR REFRESH LIVE TABLE daily_sales_uk_2022
AS
SELECT country, invoice_year, invoice_month, invoice_date,
        round(sum(unit_price * quantity),2)as total_sales
FROM LIVE.invoices
WHERE invoice_year=2022 AND country="United Kingdom"
GROUP BY country, invoice_year, invoice_month, invoice_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
-- MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>
