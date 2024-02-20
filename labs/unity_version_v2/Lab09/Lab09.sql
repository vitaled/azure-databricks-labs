-- Databricks notebook source
DROP SCHEMA IF EXISTS sandbox.bronze CASCADE;


-- COMMAND ----------

CREATE SCHEMA sandbox.bronze;

-- COMMAND ----------

SHOW TABLES IN sandbox.bronze

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW products_tmp
USING CSV
OPTIONS (path "/Volumes/sandbox/raw/data/products.csv", header "true", inferSchema "true");
CREATE TABLE sandbox.bronze.products
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT * FROM products_tmp;

-- COMMAND ----------

SELECT * FROM sandbox.bronze.products

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW customers_tmp
USING CSV
OPTIONS (path "/Volumes/sandbox/raw/data/customers.csv", header "true", inferSchema "true");
CREATE OR REPLACE TABLE sandbox.bronze.customers
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT * FROM customers_tmp;

-- COMMAND ----------

SELECT * FROM sandbox.bronze.customers

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW sales_tmp
USING CSV
OPTIONS (path "/Volumes/sandbox/raw/data/sales.csv", header "true", inferSchema "true");
CREATE OR REPLACE TABLE sandbox.bronze.sales
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT * FROM sales_tmp;

-- COMMAND ----------

SELECT * FROM sandbox.bronze.sales

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW geo_tmp
USING CSV
OPTIONS (path "/Volumes/sandbox/raw/data/geo.csv", header "true", inferSchema "true");
CREATE OR REPLACE TABLE sandbox.bronze.geo
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT * FROM geo_tmp;

-- COMMAND ----------

SELECT * FROM sandbox.bronze.geo;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW campaign_tmp
USING CSV
OPTIONS (path "/Volumes/sandbox/raw/data/campaign.csv", header "true", inferSchema "true");
CREATE OR REPLACE TABLE sandbox.bronze.campaign
USING DELTA
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

SELECT SPLIT(`Email Name`,':') FROM sandbox.bronze.customers

-- COMMAND ----------

SELECT SPLIT(`Email Name`,':')[0], SPLIT(`Email Name`,':')[1] FROM sandbox.bronze.customers

-- COMMAND ----------



SELECT LOWER(REPLACE(REPLACE(SPLIT(`Email Name`,':')[0],'(',''),')','')) AS email, SPLIT(`Email Name`,':')[1] as name FROM sandbox.bronze.customers

-- COMMAND ----------



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

SELECT min(`date`),max(`date`) FROM sandbox.silver.sales

-- COMMAND ----------

DESCRIBE sandbox.silver.sales

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd 
-- MAGIC from pyspark.sql.functions import col
-- MAGIC date_range = pd.date_range('2015-01-01','2020-06-30')
-- MAGIC df = spark.createDataFrame(pd.DataFrame(date_range, columns=['date']))
-- MAGIC df = df.withColumn('date', col('date').cast('date'))
-- MAGIC df.createOrReplaceTempView('date_tmp')

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE OR REPLACE TABLE sandbox.gold.dim_date AS
SELECT `date`,
        MONTH(`date`) AS month, 
        CONCAT('Q',QUARTER(`date`)) AS quarter,
        YEAR(`date`) AS year,
        DAYOFWEEK(`date`) AS dayofweek,
        WEEKOFYEAR(`date`) AS weekofyear,
        dayofyear(`date`) AS dayofyear
FROM date_tmp ORDER BY `date` ASC


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

SHOW TABLES IN sandbox.gold
