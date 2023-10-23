CREATE MASTER KEY;

CREATE DATABASE SCOPED CREDENTIAL ServiceIdentity WITH IDENTITY = 'Managed Identity';

CREATE EXTERNAL DATA SOURCE [ext_datasource_with_abfss]
WITH 
  ( TYPE = HADOOP , 
    LOCATION = 'abfss://export@<SOSTITUIRE CON STORAGE ACCOUNT>.dfs.core.windows.net' , 
    --LOCATION = 'abfss://<container>@<storage_account_name>.dfs.core.windows.net' , 
    CREDENTIAL = ServiceIdentity
  ) ;

CREATE EXTERNAL FILE FORMAT parquet_format
WITH
(  
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)

CREATE EXTERNAL TABLE fact_sales (
	[ProductID] int,
	[Date] date,
	[CustomerID] int,
	[CampaignID] int,
	[CatSegID] bigint,
	[Units] int
	)
	WITH (
	DATA_SOURCE = [ext_datasource_with_abfss],
	LOCATION = 'parquet/fact_sales',
	FILE_FORMAT = [parquet_format]
	)
GO

SELECT TOP(10) * FROM dbo.fact_sales;

CREATE EXTERNAL TABLE dim_date (
	[date] date,
	[month] int,
	[quarter] nvarchar(4000),
	[year] int,
	[dayofweek] int,
	[weekofyear] int,
	[weekday] int
	)
	WITH (
	LOCATION = 'parquet/dim_date',
	DATA_SOURCE = [ext_datasource_with_abfss],
	FILE_FORMAT = [parquet_format]
	)
GO

CREATE EXTERNAL TABLE dim_products (
	[ProductID] int,
	[Product] nvarchar(4000),
	[UnitCost] nvarchar(4000),
	[UnitPrice] nvarchar(4000)
	)
	WITH (
	LOCATION = 'parquet/dim_products',
	DATA_SOURCE = [ext_datasource_with_abfss],
	FILE_FORMAT = [parquet_format]
	)
GO

CREATE EXTERNAL TABLE dim_customers (
	[CustomerId] int,
	[ZipCode] int,
	[email] nvarchar(4000),
	[name] nvarchar(4000)
	)
	WITH (
	LOCATION = 'parquet/dim_customers',
	DATA_SOURCE = [ext_datasource_with_abfss],
	FILE_FORMAT = [parquet_format]
	)
GO

CREATE EXTERNAL TABLE dim_catseg (
	[CatSegID] bigint,
	[Category] nvarchar(4000),
	[Segment] nvarchar(4000)
	)
	WITH (
	LOCATION = 'parquet/dim_catseg',
	DATA_SOURCE = [ext_datasource_with_abfss],
	FILE_FORMAT = [parquet_format]
	)
GO

CREATE SCHEMA gold
GO

CREATE TABLE [gold].[dimcatseg]
WITH (
    	CLUSTERED COLUMNSTORE INDEX,
    	DISTRIBUTION = REPLICATE
	)
AS SELECT * FROM dbo.dim_catseg

CREATE TABLE gold.dimcustomers
WITH (
    	CLUSTERED COLUMNSTORE INDEX,
    	DISTRIBUTION = REPLICATE
	)
AS SELECT * FROM dbo.dim_customers

CREATE TABLE gold.dimdate
WITH (
    	CLUSTERED COLUMNSTORE INDEX,
    	DISTRIBUTION = REPLICATE
	)
AS SELECT * FROM dbo.dim_date

CREATE TABLE gold.dimproducts
WITH (
    	CLUSTERED COLUMNSTORE INDEX,
    	DISTRIBUTION = HASH([ProductID])
	)
AS SELECT * FROM dbo.dim_products

CREATE TABLE gold.factsales
WITH (
    	CLUSTERED COLUMNSTORE INDEX,
    	DISTRIBUTION = HASH([ProductID])
	)
AS SELECT * FROM dbo.fact_sales


SELECT TOP(10) * FROM gold.factsales
