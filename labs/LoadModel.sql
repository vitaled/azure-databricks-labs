-- The statement "CREATE MASTER KEY" is used Dedicated SQL pool to create a master encryption key. 
-- This master key is essential for protecting sensitive data stored in the database by encrypting it. 
CREATE MASTER KEY;

-- The statement "CREATE DATABASE SCOPED CREDENTIAL" is used to create a database-scoped credential in SQL Server. 
-- This is a useful feature when working with external data sources or when you need to access resources outside of the database.
--
-- Here's an explanation of this statement:
-- 
-- CREATE DATABASE SCOPED CREDENTIAL: This is a DDL (Data Definition Language) statement used to create a database-scoped credential.
-- A database-scoped credential is a way to securely store and manage credentials, such as usernames and passwords, within a database for authentication purposes.
-- These credentials can be used to access external data sources, such as Azure Blob Storage, Azure Data Lake Storage, or other database instances.
--
-- ServiceIdentity: This is the name of the database-scoped credential you are creating. You can choose a meaningful name that reflects the purpose of the credential.
--
-- WITH IDENTITY = 'Managed Identity': This part of the statement specifies the type of identity or authentication mechanism associated with the credential.
--                                     In this case, it indicates that the credential is linked to a managed identity.
--                                     A managed identity is a feature in Azure that provides an identity to a resource, such as an Azure Virtual Machine, Azure Function, or Azure App Service. 
--                                     This identity can be used to access other Azure resources securely without the need to store explicit credentials.
--                                     By using 'Managed Identity,' you're indicating that the database-scoped credential is associated with a managed identity 
--                                     and can be used to access external resources on behalf of that managed identity.
CREATE DATABASE SCOPED CREDENTIAL ServiceIdentity WITH IDENTITY = 'Managed Identity';


-- The provided T-SQL statement creates an external data source in SQL Server for connecting to a data source located in Azure Data Lake Storage Gen2 using the ABFSS (Azure Blob File System Secure) protocol. 
--
-- Here's an explanation of each part of the statement:
-- 
-- CREATE EXTERNAL DATA SOURCE [ext_datasource_with_abfss]: This part of the statement creates an external data source with the name "ext_datasource_with_abfss." This data source will be used to connect to external data stored in Azure Data Lake Storage Gen2.
-- 
-- WITH:
-- 		TYPE = HADOOP: The TYPE parameter specifies the type of the external data source, which, in this case, is set to "HADOOP." This is used when connecting to Azure Data Lake Storage Gen2 with the ABFSS protocol.
-- 		LOCATION = 'abfss://export@[HERE PUT YOUR STORAGE ACCOUNT NAME].dfs.core.windows.net': The LOCATION parameter defines the URI of the external data source. In this case, it points to an Azure Data Lake Storage Gen2 container. You should replace "[HERE PUT YOUR STORAGE ACCOUNT NAME]" with the actual name of your Azure Storage account. This URI format is specific to the ABFSS protocol for Azure Data Lake Storage Gen2.
-- 		CREDENTIAL = ServiceIdentity: The CREDENTIAL parameter specifies the previously created database-scoped credential named "ServiceIdentity." This credential is used for authentication when accessing the external data source.

CREATE EXTERNAL DATA SOURCE [ext_datasource_with_abfss] WITH (
	TYPE = HADOOP,
	LOCATION = 'abfss://export@[HERE PUT YOUR STORAGE ACCOUNT NAME].dfs.core.windows.net',
	--LOCATION = 'abfss://<container>@<storage_account_name>.dfs.core.windows.net' , 
	CREDENTIAL = ServiceIdentity
);

CREATE EXTERNAL FILE FORMAT parquet_format WITH (
	FORMAT_TYPE = PARQUET,
	DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
) CREATE EXTERNAL TABLE fact_sales (
	[ProductID] int,
	[Date] date,
	[CustomerID] int,
	[CampaignID] int,
	[CatSegID] bigint,
	[Units] int
) WITH (
	DATA_SOURCE = [ext_datasource_with_abfss],
	LOCATION = 'parquet/fact_sales',
	FILE_FORMAT = [parquet_format]
)
GO
SELECT
	TOP(10) *
FROM
	dbo.fact_sales;

CREATE EXTERNAL TABLE dim_date (
	[date] date,
	[month] int,
	[quarter] nvarchar(4000),
	[year] int,
	[dayofweek] int,
	[weekofyear] int,
	[weekday] int
) WITH (
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
	) WITH (
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
	) WITH (
		LOCATION = 'parquet/dim_customers',
		DATA_SOURCE = [ext_datasource_with_abfss],
		FILE_FORMAT = [parquet_format]
	)
GO
	CREATE EXTERNAL TABLE dim_catseg (
		[CatSegID] bigint,
		[Category] nvarchar(4000),
		[Segment] nvarchar(4000)
	) WITH (
		LOCATION = 'parquet/dim_catseg',
		DATA_SOURCE = [ext_datasource_with_abfss],
		FILE_FORMAT = [parquet_format]
	)
GO
	CREATE SCHEMA gold
GO
	CREATE TABLE [gold].[dimcatseg] WITH (
		CLUSTERED COLUMNSTORE INDEX,
		DISTRIBUTION = REPLICATE
	) AS
SELECT
	*
FROM
	dbo.dim_catseg CREATE TABLE gold.dimcustomers WITH (
		CLUSTERED COLUMNSTORE INDEX,
		DISTRIBUTION = REPLICATE
	) AS
SELECT
	*
FROM
	dbo.dim_customers CREATE TABLE gold.dimdate WITH (
		CLUSTERED COLUMNSTORE INDEX,
		DISTRIBUTION = REPLICATE
	) AS
SELECT
	*
FROM
	dbo.dim_date CREATE TABLE gold.dimproducts WITH (
		CLUSTERED COLUMNSTORE INDEX,
		DISTRIBUTION = HASH([ProductID])
	) AS
SELECT
	*
FROM
	dbo.dim_products CREATE TABLE gold.factsales WITH (
		CLUSTERED COLUMNSTORE INDEX,
		DISTRIBUTION = HASH([ProductID])
	) AS
SELECT
	*
FROM
	dbo.fact_sales
SELECT
	TOP(10) *
FROM
	gold.factsales