-- Databricks notebook source
DROP CATALOG IF EXISTS sandbox CASCADE;

-- COMMAND ----------

SET `storage.account.name`=`staadbmeta001`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC storage_credential_name = spark.sql('SHOW STORAGE CREDENTIALS').take(1)[0]['name']
-- MAGIC display(spark.sql(f"SET `storage.credential.name`={storage_credential_name}"))

-- COMMAND ----------

DROP EXTERNAL LOCATION raw_external_location;
CREATE EXTERNAL LOCATION raw_external_location
URL 'abfss://raw@${storage.account.name}.dfs.core.windows.net/'
 WITH (STORAGE CREDENTIAL `${storage.credential.name}`);

-- COMMAND ----------

DROP EXTERNAL LOCATION export_external_location;
 CREATE EXTERNAL LOCATION export_external_location
 URL 'abfss://export@${storage.account.name}.dfs.core.windows.net/'
 WITH (STORAGE CREDENTIAL  `${storage.credential.name}`);

-- COMMAND ----------

SHOW EXTERNAL LOCATIONS

-- COMMAND ----------


CREATE CATALOG sandbox;

-- COMMAND ----------

DROP SCHEMA IF EXISTS sandbox.bronze CASCADE;
CREATE SCHEMA sandbox.bronze;

-- COMMAND ----------

SHOW TABLES IN sandbox.bronze

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW products_tmp
USING CSV
OPTIONS (path "abfss://raw@${storage.account.name}.dfs.core.windows.net/products.csv", header "true", inferSchema "true");
CREATE TABLE sandbox.bronze.products
USING DELTA
LOCATION 'abfss://export@${storage.account.name}.dfs.core.windows.net/delta/products'
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT * FROM products_tmp;

-- COMMAND ----------

SELECT * FROM sandbox.bronze.products

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW customers_tmp
USING CSV
OPTIONS (path "abfss://raw@${storage.account.name}.dfs.core.windows.net/customers.csv", header "true", inferSchema "true");
CREATE OR REPLACE TABLE sandbox.bronze.customers
USING DELTA
LOCATION 'abfss://export@${storage.account.name}.dfs.core.windows.net/delta/customers'
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT * FROM customers_tmp;

-- COMMAND ----------

SELECT * FROM sandbox.bronze.customers

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW sales_tmp
USING CSV
OPTIONS (path "abfss://raw@${storage.account.name}.dfs.core.windows.net/sales.csv", header "true", inferSchema "true");
CREATE OR REPLACE TABLE sandbox.bronze.sales
USING DELTA
LOCATION 'abfss://export@${storage.account.name}.dfs.core.windows.net/delta/sales'
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT * FROM sales_tmp;

-- COMMAND ----------

SELECT * FROM sandbox.bronze.sales

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW geo_tmp
USING CSV
OPTIONS (path "abfss://raw@${storage.account.name}.dfs.core.windows.net/geo.csv", header "true", inferSchema "true");
CREATE OR REPLACE TABLE sandbox.bronze.geo
USING DELTA
LOCATION 'abfss://export@${storage.account.name}.dfs.core.windows.net/delta/geo'

TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT * FROM geo_tmp;

-- COMMAND ----------

SELECT * FROM sandbox.bronze.geo;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW campaign_tmp
USING CSV
OPTIONS (path "abfss://raw@${storage.account.name}.dfs.core.windows.net/campaign.csv", header "true", inferSchema "true");
CREATE OR REPLACE TABLE sandbox.bronze.campaign
USING DELTA
LOCATION 'abfss://export@${storage.account.name}.dfs.core.windows.net/delta/campaign'
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT * FROM campaign_tmp;

-- COMMAND ----------

SELECT * FROM sandbox.bronze.campaign;

-- COMMAND ----------

SHOW TABLES IN sandbox.bronze

-- COMMAND ----------

DROP SCHEMA IF EXISTS sandbox.silver CASCADE;
CREATE DATABASE sandbox.silver;

-- COMMAND ----------

SELECT LOWER(REPLACE(REPLACE(SPLIT(`Email Name`,':')[0],'(',''),')','')), SPLIT(`Email Name`,':')[1] FROM sandbox.bronze.customers

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.silver.customers
USING DELTA
AS
SELECT CustomerId,ZipCode, LOWER(REPLACE(REPLACE(SPLIT(`Email Name`,':')[0],'(',''),')','')) AS email, SPLIT(`Email Name`,':')[1] AS name FROM sandbox.bronze.customers 

-- COMMAND ----------

SELECT * FROM sandbox.silver.customers

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.silver.products
USING DELTA
AS
SELECT ProductID,
       Product,
       Category,
       Segment,
       FORMAT_NUMBER(`Unit Cost`,'####.##') AS `UnitCost`,
       FORMAT_NUMBER(`Unit Price`,'####.##') AS `UnitPrice`
FROM sandbox.bronze.products

-- COMMAND ----------

SELECT * FROM sandbox.silver.products

-- COMMAND ----------

SELECT * FROM sandbox.bronze.geo

-- COMMAND ----------


CREATE OR REPLACE TABLE sandbox.silver.geo
USING DELTA
AS
SELECT STRING(Zip),SPLIT(City,',')[0] AS City ,State,Region,District,Country FROM sandbox.bronze.geo

-- COMMAND ----------

SELECT * FROM sandbox.silver.geo

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.silver.campaign
USING DELTA
AS
SELECT * FROM sandbox.bronze.campaign

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.silver.sales
USING DELTA
AS
SELECT * FROM sandbox.bronze.sales

-- COMMAND ----------

SHOW TABLES IN sandbox.silver

-- COMMAND ----------

DROP SCHEMA IF EXISTS sandbox.gold CASCADE;
CREATE SCHEMA sandbox.gold;

-- COMMAND ----------

SELECT * FROM sandbox.silver.sales

-- COMMAND ----------

SELECT DISTINCT to_date(`date`,'M/d/yyyy') AS date
FROM sandbox.silver.sales ORDER BY date ASC

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.gold.dim_date
SELECT `date`,
        MONTH(`date`) AS month, 
        CONCAT('Q',QUARTER(`date`)) AS quarter,
        YEAR(`date`) AS year,
        DAYOFWEEK(`date`) AS dayofweek,
        WEEKOFYEAR(`date`) AS weekofyear,
        WEEKDAY(`date`) AS weekday
FROM(
SELECT DISTINCT to_date(`date`,'M/d/yyyy') AS date
FROM sandbox.silver.sales ORDER BY date ASC
)

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.gold.dim_date
SELECT `date`,
        MONTH(`date`) AS month, 
        CONCAT('Q',QUARTER(`date`)) AS quarter,
        YEAR(`date`) AS year,
        DAYOFWEEK(`date`) AS dayofweek,
        WEEKOFYEAR(`date`) AS weekofyear,
        WEEKDAY(`date`) AS weekday
FROM(
SELECT DISTINCT to_date(`date`,'M/d/yyyy') AS date
FROM sandbox.silver.sales ORDER BY date ASC
)

-- COMMAND ----------

SELECT * FROM sandbox.gold.dim_date

-- COMMAND ----------

SELECT * FROM sandbox.silver.products

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.gold.dim_catseg AS
SELECT monotonically_increasing_id() as CatSegID
,Category,Segment
FROM (
SELECT DISTINCT Category,Segment FROM sandbox.silver.products
)

-- COMMAND ----------

SELECT * FROM sandbox.gold.dim_catseg

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.gold.fact_sales 
AS
SELECT ss.ProductID,to_date(`date`,'M/d/yyyy') as Date,CustomerID,CampaignID,CatSegID,Units
FROM sandbox.silver.sales ss 
LEFT JOIN 
(
  SELECT * 
  FROM sandbox.silver.products sp
  LEFT JOIN sandbox.gold.dim_catseg dcs
  WHERE sp.Category=dcs.Category AND sp.Segment=dcs.Segment
) sp_dcs
WHERE sp_dcs.ProductID=ss.ProductID


-- COMMAND ----------

SELECT * FROM  sandbox.gold.fact_sales 

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.gold.dim_products
AS
SELECT ProductID,Product,UnitCost,UnitPrice
FROM sandbox.silver.products

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.gold.dim_customers
AS
SELECT *
FROM sandbox.silver.customers

-- COMMAND ----------

SELECT * FROM sandbox.silver.geo
-- CREATE OR REPLACE TABLE gold.geo
-- AS
-- SELECT ProductID,Product,UnitCost,UnitPrice
-- FROM silver.geo

-- COMMAND ----------

SHOW TABLES IN sandbox.gold

-- COMMAND ----------

SELECT quarter,SUM(Units) unit_solds
FROM sandbox.gold.fact_sales gs
LEFT JOIN sandbox.gold.dim_products gp ON gs.ProductId=gp.ProductId
LEFT JOIN sandbox.gold.dim_date dd ON gs.Date=dd.date
GROUP BY quarter
ORDER BY quarter ASC


-- COMMAND ----------

SELECT dayofweek,SUM(Units) unit_solds
FROM sandbox.gold.fact_sales gs
LEFT JOIN sandbox.gold.dim_products gp ON gs.ProductId=gp.ProductId
LEFT JOIN sandbox.gold.dim_date dd ON gs.Date=dd.date
GROUP BY dayofweek
ORDER BY dayofweek ASC


-- COMMAND ----------

SELECT gs.date,SUM(Units) unit_solds
FROM sandbox.gold.fact_sales gs
LEFT JOIN sandbox.gold.dim_products gp ON gs.ProductId=gp.ProductId
LEFT JOIN sandbox.gold.dim_date dd ON gs.Date=dd.date
GROUP BY gs.date
ORDER BY gs.date ASC

-- COMMAND ----------

SELECT month, SUM(Units*UnitPrice) - SUM(Units*UnitCost) as profit
FROM sandbox.gold.fact_sales gs
LEFT JOIN sandbox.gold.dim_products gp ON gs.ProductId=gp.ProductId
LEFT JOIN sandbox.gold.dim_date dd ON gs.Date=dd.date
GROUP BY month
ORDER BY month ASC

-- COMMAND ----------

SELECT quarter,year, COUNT(Units) sales
FROM sandbox.gold.fact_sales gs
LEFT JOIN sandbox.gold.dim_products gp ON gs.ProductId=gp.ProductId
LEFT JOIN sandbox.gold.dim_date dd ON gs.Date=dd.date
GROUP BY quarter,year
ORDER BY quarter,year ASC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SHOW TABLES IN sandbox.gold

-- COMMAND ----------



-- COMMAND ----------

CREATE SCHEMA sandbox.`export`

-- COMMAND ----------

CREATE TABLE sandbox.`export`.dim_catseg_export
USING PARQUET
LOCATION 'abfss://export@${storage.account.name}.dfs.core.windows.net/parquet/dim_catseg'
AS SELECT * FROM sandbox.gold.dim_catseg;
DROP TABLE sandbox.`export`.dim_catseg_export

-- COMMAND ----------

CREATE TABLE  sandbox.`export`.dim_customers_export
USING PARQUET
LOCATION 'abfss://export@${storage.account.name}.dfs.core.windows.net/parquet/dim_customers'
AS SELECT * FROM sandbox.gold.dim_customers;
DROP TABLE  sandbox.`export`.dim_customers_export

-- COMMAND ----------

CREATE TABLE  sandbox.`export`.dim_date_export
USING PARQUET
LOCATION 'abfss://export@${storage.account.name}.dfs.core.windows.net/parquet/dim_date'
AS SELECT * FROM sandbox.gold.dim_date;
DROP TABLE  sandbox.`export`.dim_date_export

-- COMMAND ----------

CREATE TABLE  sandbox.`export`.dim_products_export
USING PARQUET
LOCATION 'abfss://export@${storage.account.name}.dfs.core.windows.net/parquet/dim_products'
AS SELECT * FROM sandbox.gold.dim_products;
DROP TABLE  sandbox.`export`.dim_products_export

-- COMMAND ----------


CREATE TABLE  sandbox.`export`.fact_sales_export
USING PARQUET
LOCATION 'abfss://export@${storage.account.name}.dfs.core.windows.net/parquet/fact_sales'
AS SELECT * FROM sandbox.gold.fact_sales;
DROP TABLE  sandbox.`export`.fact_sales_export
