-- Databricks notebook source
-- MAGIC %python
-- MAGIC STORAGE_ACCOUNT = '<>'
-- MAGIC RAW_CONTAINER = 'raw'
-- MAGIC EXPORT_CONTAINER = 'export'
-- MAGIC print(f"abfss://{RAW_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC configs = {
-- MAGIC   "fs.azure.account.auth.type": "CustomAccessToken",
-- MAGIC   "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
-- MAGIC }
-- MAGIC
-- MAGIC try:
-- MAGIC     dbutils.fs.unmount(f"/mnt/workshop/{RAW_CONTAINER}")
-- MAGIC     # Optionally, you can add <directory-name> to the source URI of your mount point.
-- MAGIC     dbutils.fs.mount(
-- MAGIC       source = f"abfss://{RAW_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/",
-- MAGIC       mount_point = f"/mnt/workshop/{RAW_CONTAINER}",
-- MAGIC       extra_configs = configs)
-- MAGIC except: 
-- MAGIC     dbutils.fs.mount(
-- MAGIC       source = f"abfss://{RAW_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/",
-- MAGIC       mount_point = f"/mnt/workshop/{RAW_CONTAINER}",
-- MAGIC       extra_configs = configs)
-- MAGIC     
-- MAGIC try:
-- MAGIC     dbutils.fs.unmount(f"/mnt/workshop/{EXPORT_CONTAINER}")
-- MAGIC     dbutils.fs.mount(
-- MAGIC       source = f"abfss://{EXPORT_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/",
-- MAGIC       mount_point = f"/mnt/workshop/{EXPORT_CONTAINER}",
-- MAGIC       extra_configs = configs)
-- MAGIC except:    
-- MAGIC   # Optionally, you can add <directory-name> to th  e source URI of your mount point.
-- MAGIC   dbutils.fs.mount(
-- MAGIC       source = f"abfss://{EXPORT_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/",
-- MAGIC       mount_point = f"/mnt/workshop/{EXPORT_CONTAINER}",
-- MAGIC       extra_configs = configs)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/mnt/workshop/raw'))

-- COMMAND ----------

DROP DATABASE IF EXISTS bronze CASCADE;
CREATE DATABASE bronze;

-- COMMAND ----------

Show tables in bronze

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW products_tmp
USING CSV
OPTIONS (path "/mnt/workshop/raw/products.csv", header "true", inferSchema "true");
CREATE TABLE bronze.products
USING DELTA
LOCATION '/mnt/workshop/export/delta/products'
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT * FROM products_tmp;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW customers_tmp
USING CSV
OPTIONS (path "/mnt/workshop/raw/customers.csv", header "true", inferSchema "true");
CREATE OR REPLACE TABLE bronze.customers
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT * FROM customers_tmp;

-- COMMAND ----------

SELECT * FROM bronze.customers

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW sales_tmp
USING CSV
OPTIONS (path "/mnt/workshop/raw/sales.csv", header "true", inferSchema "true");
CREATE OR REPLACE TABLE bronze.sales
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT * FROM sales_tmp;

-- COMMAND ----------

SELECT * FROM bronze.sales

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW geo_tmp
USING CSV
OPTIONS (path "/mnt/workshop/raw/geo.csv", header "true", inferSchema "true");
CREATE OR REPLACE TABLE bronze.geo
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT * FROM geo_tmp;

-- COMMAND ----------

SELECT * FROM bronze.geo;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW campaign_tmp
USING CSV
OPTIONS (path "/mnt/workshop/raw/campaign.csv", header "true", inferSchema "true");
CREATE OR REPLACE TABLE bronze.campaign
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT * FROM campaign_tmp;

-- COMMAND ----------

SELECT * FROM bronze.campaign;

-- COMMAND ----------

SHOW TABLES IN bronze

-- COMMAND ----------

DROP DATABASE IF EXISTS silver CASCADE;
CREATE DATABASE silver;

-- COMMAND ----------

SELECT LOWER(REPLACE(REPLACE(SPLIT(`Email Name`,':')[0],'(',''),')','')), SPLIT(`Email Name`,':')[1]FROM bronze.customers

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.customers
USING DELTA
AS
SELECT CustomerId,ZipCode, LOWER(REPLACE(REPLACE(SPLIT(`Email Name`,':')[0],'(',''),')','')) AS email, SPLIT(`Email Name`,':')[1] AS name FROM bronze.customers 

-- COMMAND ----------

SELECT * FROM silver.customers

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.products
USING DELTA
AS
SELECT ProductID,
       Product,
       Category,
       Segment,
       FORMAT_NUMBER(`Unit Cost`,'####.##') AS `UnitCost`,
       FORMAT_NUMBER(`Unit Price`,'####.##') AS `UnitPrice`
FROM bronze.products

-- COMMAND ----------

SELECT * FROM silver.products

-- COMMAND ----------

SELECT * FROM bronze.geo

-- COMMAND ----------


CREATE OR REPLACE TABLE silver.geo
USING DELTA
AS
SELECT STRING(Zip),SPLIT(City,',')[0] AS City ,State,Region,District,Country FROM bronze.geo

-- COMMAND ----------

SELECT * FROM silver.geo

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.campaign
USING DELTA
AS
SELECT * FROM bronze.campaign

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.sales
USING DELTA
AS
SELECT * FROM bronze.sales

-- COMMAND ----------

SHOW TABLES IN silver

-- COMMAND ----------

DROP DATABASE IF EXISTS gold CASCADE;
CREATE DATABASE gold;

-- COMMAND ----------

SELECT * FROM silver.sales

-- COMMAND ----------

SELECT DISTINCT to_date(`date`,'M/d/yyyy') AS date
FROM silver.sales ORDER BY date ASC

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_date
SELECT `date`,
        MONTH(`date`) AS month, 
        CONCAT('Q',QUARTER(`date`)) AS quarter,
        YEAR(`date`) AS year,
        DAYOFWEEK(`date`) AS dayofweek,
        WEEKOFYEAR(`date`) AS weekofyear,
        WEEKDAY(`date`) AS weekday
FROM(
SELECT DISTINCT to_date(`date`,'M/d/yyyy') AS date
FROM silver.sales ORDER BY date ASC
)

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_date
SELECT `date`,
        MONTH(`date`) AS month, 
        CONCAT('Q',QUARTER(`date`)) AS quarter,
        YEAR(`date`) AS year,
        DAYOFWEEK(`date`) AS dayofweek,
        WEEKOFYEAR(`date`) AS weekofyear,
        WEEKDAY(`date`) AS weekday
FROM(
SELECT DISTINCT to_date(`date`,'M/d/yyyy') AS date
FROM silver.sales ORDER BY date ASC
)

-- COMMAND ----------

SELECT * FROM gold.dim_date

-- COMMAND ----------

SELECT * FROM silver.products

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_catseg AS
SELECT monotonically_increasing_id() as CatSegID
,Category,Segment
FROM (
SELECT DISTINCT Category,Segment FROM silver.products
)

-- COMMAND ----------

SELECT * FROM gold.dim_catseg

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.fact_sales 
AS
SELECT ss.ProductID,to_date(`date`,'M/d/yyyy') as Date,CustomerID,CampaignID,CatSegID,Units
FROM silver.sales ss 
LEFT JOIN 
(
  SELECT * 
  FROM silver.products sp
  LEFT JOIN gold.dim_catseg dcs
  WHERE sp.Category=dcs.Category AND sp.Segment=dcs.Segment
) sp_dcs
WHERE sp_dcs.ProductID=ss.ProductID


-- COMMAND ----------

SELECT * FROM  gold.fact_sales 

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_products
AS
SELECT ProductID,Product,UnitCost,UnitPrice
FROM silver.products

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_customers
AS
SELECT *
FROM silver.customers

-- COMMAND ----------

SELECT * FROM silver.geo
-- CREATE OR REPLACE TABLE gold.geo
-- AS
-- SELECT ProductID,Product,UnitCost,UnitPrice
-- FROM silver.geo

-- COMMAND ----------

SHOW TABLES IN gold

-- COMMAND ----------

SELECT quarter,SUM(Units) unit_solds
FROM gold.fact_sales gs
LEFT JOIN gold.dim_products gp ON gs.ProductId=gp.ProductId
LEFT JOIN gold.dim_date dd ON gs.Date=dd.date
GROUP BY quarter
ORDER BY quarter ASC


-- COMMAND ----------

SELECT dayofweek,SUM(Units) unit_solds
FROM gold.fact_sales gs
LEFT JOIN gold.dim_products gp ON gs.ProductId=gp.ProductId
LEFT JOIN gold.dim_date dd ON gs.Date=dd.date
GROUP BY dayofweek
ORDER BY dayofweek ASC


-- COMMAND ----------

SELECT gs.date,SUM(Units) unit_solds
FROM gold.fact_sales gs
LEFT JOIN gold.dim_products gp ON gs.ProductId=gp.ProductId
LEFT JOIN gold.dim_date dd ON gs.Date=dd.date
GROUP BY gs.date
ORDER BY gs.date ASC

-- COMMAND ----------

SELECT month, SUM(Units*UnitPrice) - SUM(Units*UnitCost) as profit
FROM gold.fact_sales gs
LEFT JOIN gold.dim_products gp ON gs.ProductId=gp.ProductId
LEFT JOIN gold.dim_date dd ON gs.Date=dd.date
GROUP BY month
ORDER BY month ASC

-- COMMAND ----------

SELECT quarter,year, COUNT(Units) sales
FROM gold.fact_sales gs
LEFT JOIN gold.dim_products gp ON gs.ProductId=gp.ProductId
LEFT JOIN gold.dim_date dd ON gs.Date=dd.date
GROUP BY quarter,year
ORDER BY quarter,year ASC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SHOW TABLES IN gold

-- COMMAND ----------

CREATE TABLE dim_catseg_export
USING PARQUET
LOCATION '/mnt/workshop/export/parquet/dim_catseg'
AS SELECT * FROM gold.dim_catseg;
DROP TABLE dim_catseg_export

-- COMMAND ----------

CREATE TABLE dim_customers_export
USING PARQUET
LOCATION '/mnt/workshop/export/parquet/dim_customers'
AS SELECT * FROM gold.dim_customers;
DROP TABLE dim_customers_export

-- COMMAND ----------

CREATE TABLE dim_date_export
USING PARQUET
LOCATION '/mnt/workshop/export/parquet/dim_date'
AS SELECT * FROM gold.dim_date;
DROP TABLE dim_date_export

-- COMMAND ----------

CREATE TABLE dim_products_export
USING PARQUET
LOCATION '/mnt/workshop/export/parquet/dim_products'
AS SELECT * FROM gold.dim_products;
DROP TABLE dim_products_export

-- COMMAND ----------


CREATE TABLE fact_sales_export
USING PARQUET
LOCATION '/mnt/workshop/export/parquet/fact_sales'
AS SELECT * FROM gold.fact_sales;
DROP TABLE fact_sales_export
