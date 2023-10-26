-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.rm('dbfs:/mnt/workshop/diamonds_csv',recurse=True)
-- MAGIC dbutils.fs.rm('dbfs:/mnt/workshop/diamonds_delta',recurse=True)
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS default.diamonds_csv;
DROP TABLE IF EXISTS default.diamonds_csv_folder;
DROP TABLE IF EXISTS default.diamonds_delta;
DROP TABLE IF EXISTS default.diamonds_delta_new;
DROP VIEW IF EXISTS diamonds_csv_temp ;

-- COMMAND ----------

CREATE TABLE default.diamonds_csv
USING CSV
LOCATION "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"
OPTIONS(
  HEADER = True,
  INFERSCHEMA=True
)


-- COMMAND ----------

SELECT * FROM default.diamonds_csv 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Understanding the INSERT INTO TABLE Statement
-- MAGIC
-- MAGIC The SQL statement `INSERT INTO TABLE default.diamonds_csv VALUES(53941, 0.75, 'Ideal', 'D', 'SI2', 62.2, 55, 2757, 5.83, 5.87, 3.64)` is an operation to insert a new data row into a table named "default.diamonds_csv." Here's a breakdown of the statement:
-- MAGIC
-- MAGIC `INSERT INTO TABLE default.diamonds_csv`: This part of the statement specifies the target table into which you want to insert data. In this case, the target table is "default.diamonds_csv."
-- MAGIC
-- MAGIC `VALUES(53941, 0.75, 'Ideal', 'D', 'SI2', 62.2, 55, 2757, 5.83, 5.87, 3.64)`: These are the values you intend to insert into the table. Each value corresponds to a column in the "default.diamonds_csv" table in the order in which the columns are defined. Let's break down the values:
-- MAGIC
-- MAGIC - 53941: This is the value for the first column in the table.
-- MAGIC - 0.75: This is the value for the second column.
-- MAGIC - 'Ideal': This is a string value for the third column.
-- MAGIC - 'D': This is a string value for the fourth column.
-- MAGIC - 'SI2': This is a string value for the fifth column.
-- MAGIC - 62.2: This is a numeric value for the sixth column.
-- MAGIC - 55: This is a numeric value for the seventh column.
-- MAGIC - 2757: This is a numeric value for the eighth column.
-- MAGIC - 5.83: This is a numeric value for the ninth column.
-- MAGIC - 5.87: This is a numeric value for the tenth column.
-- MAGIC - 3.64: This is a numeric value for the eleventh column.
-- MAGIC
-- MAGIC When this SQL statement is executed, a new row with the specified values will be added to the "default.diamonds_csv" table. Each value will be placed in its respective column in the order they appear in the "VALUES" clause.
-- MAGIC
-- MAGIC This operation is a common way to insert individual records or rows of data into a table in a relational database or data processing environment like SparkSQL, and it allows you to expand your dataset with new data as needed.

-- COMMAND ----------

INSERT INTO TABLE default.diamonds_csv VALUES(53941,0.75,'Ideal','D','SI2',62.2,55,2757,5.83,5.87,3.64)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Error Explanation: Attempting to Insert into a CSV File
-- MAGIC
-- MAGIC The error message you received, "Cannot insert into dbfs:/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv, as it is a file instead of a directory," is generated because you are trying to insert data into a CSV file, rather than a directory.
-- MAGIC
-- MAGIC When you initially created the table "default.diamonds_csv," you used the USING CSV statement and provided a file location for your data, which pointed to a specific CSV file, "diamonds.csv." This file location was treated as the source of your data table.
-- MAGIC
-- MAGIC However, in the SQL statement, you're attempting to insert new values into this table, which is supposed to be a collection of data records. A table in SparkSQL should be associated with a directory where data files (e.g., multiple CSV files) reside. It's common to have a directory with multiple files containing different parts of your dataset.
-- MAGIC
-- MAGIC In this case, the system is interpreting "diamonds.csv" as an individual file, not a directory with multiple data records. To resolve this issue, you should provide a directory path in the "USING CSV LOCATION" when creating the table, and each data record should be in a separate file within that directory. This way, you can insert new records into the table without encountering the error.
-- MAGIC
-- MAGIC In summary, the error occurs because you are trying to insert data into a CSV file directly, but a table in SparkSQL should be associated with a directory where data files are organized. To insert data into a table, the table should be created with a directory path and should have multiple files representing data records.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE TABLE diamonds_csv_folder
USING CSV 
LOCATION '/mnt/workshop/diamonds_csv/'
OPTIONS(
  HEADER = True
)
AS 
SELECT * FROM default.diamonds_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/mnt/workshop/diamonds_csv/'))

-- COMMAND ----------

INSERT INTO TABLE default.diamonds_csv_folder VALUES(53941,0.75,'Ideal','D','SI2',62.2,55,2757,5.83,5.87,3.64)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/mnt/workshop/diamonds_csv/'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Understanding the UPDATE Statement
-- MAGIC
-- MAGIC The SQL statement `UPDATE default.diamonds_csv_folder SET cut='Wonderful' WHERE _c0=1` is used to modify existing data in a table. 
-- MAGIC
-- MAGIC Here's a breakdown of the statement:
-- MAGIC
-- MAGIC `UPDATE default.diamonds_csv_folder`: This part of the statement specifies the target table you want to update. In this case, the target table is "default.diamonds_csv_folder."
-- MAGIC
-- MAGIC `SET cut='Wonderful'`: This part of the statement indicates which column you want to update and what value to assign to it. It sets the value of the "cut" column to 'Wonderful.'
-- MAGIC
-- MAGIC `WHERE _c0=1`: This is the condition that specifies which rows in the table should be updated. In this case, the condition is that the "_c0" column must have a value equal to 1.
-- MAGIC
-- MAGIC When this SQL statement is executed, it will search the "default.diamonds_csv_folder" table for rows where the value in the "_c0" column is equal to 1. For all matching rows, it will update the "cut" column with the value 'Wonderful.'
-- MAGIC
-- MAGIC This type of SQL statement is used to modify or update existing data in a database table. It allows you to make changes to specific records in the table based on defined criteria. In this example, the "cut" column is being updated for the row where "_c0" is equal to 1, setting the "cut" value to 'Wonderful.'
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

UPDATE default.diamonds_csv_folder SET cut='Wonderful' WHERE _c0=1

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Error explanation: Attempt to update a not Delta Lake table
-- MAGIC
-- MAGIC The error message "Error in SQL statement: AnalysisException: UPDATE destination only supports Delta sources" occurs because you are trying to perform an UPDATE operation on a table created from a non-Delta source (in this case, a table created using the "USING CSV" format).
-- MAGIC
-- MAGIC Delta Lake is an open-source storage layer that brings ACID transactions to Apache Spark and big data workloads. It provides advanced data management features such as transactional capabilities, schema evolution, and more. Delta Lake tables are designed to support atomic updates and deletes, making them suitable for "UPDATE" and "DELETE" operations.
-- MAGIC
-- MAGIC However, when you create a table using the "USING CSV" format as in your example, it's not a Delta Lake table. It's a regular table backed by CSV files, which do not support direct update operations like Delta tables.
-- MAGIC
-- MAGIC To resolve the issue and perform updates on this table, you have a few options:
-- MAGIC
-- MAGIC Convert to Delta Lake: If you intend to perform updates and other advanced operations on the table, you can convert it to a Delta Lake table. This involves creating a new Delta table, importing the data from the CSV table, and then you can perform updates on it.
-- MAGIC
-- MAGIC Use Traditional CSV Update: If you want to update specific values in the CSV-backed table, you would typically read the data, apply the updates in your Spark code, and then write the modified data back to the CSV files. This approach does not perform in-place updates but rather creates a new version of the data with the changes.
-- MAGIC
-- MAGIC In summary, the error occurs because you are trying to update a table created from a non-Delta source, and the table does not support direct update operations. To perform updates, you can either convert the table to a Delta Lake table or use a traditional approach to update the data in your Spark code and write the modified data back to the CSV files.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a Temporary View from a CSV File: `CREATE TEMPORARY VIEW`
-- MAGIC
-- MAGIC The command `CREATE TEMPORARY VIEW diamonds_csv_temp USING CSV OPTIONS (path='/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv', inferSchema=True, header=True)` is used to create a temporary view in SparkSQL. 
-- MAGIC
-- MAGIC Here's a breakdown of the statement:
-- MAGIC
-- MAGIC `CREATE TEMPORARY VIEW diamonds_csv_temp`: This part of the command initializes the creation of a temporary view in SparkSQL. Temporary views are in-memory data representations that allow you to interact with and query data without the need to create a physical table. "diamonds_csv_temp" is the name assigned to this temporary view.
-- MAGIC
-- MAGIC `USING CSV`: this clause specifies the format of the data source. In this case, it indicates that the data to be used for the view is in CSV (Comma-Separated Values) format.
-- MAGIC
-- MAGIC `OPTIONS (path='/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv', inferSchema=True, header=True)`: this clause defines specific settings for the CSV data source:
-- MAGIC
-- MAGIC - path: This parameter specifies the file path to the CSV data source. In this instance, it points to the "diamonds.csv" file located at '/databricks-datasets/Rdatasets/data-001/csv/ggplot2/'.
-- MAGIC - inferSchema=True: When set to "True," this parameter instructs Spark to automatically infer the data types of the columns in the CSV file. This means that Spark will attempt to identify whether each column contains numeric, string, or other types of data.
-- MAGIC - header=True: This parameter, when set to "True," indicates that the first row in the CSV file contains the column headers. It's a common practice in CSV files to use the first row as headers to identify each column's name.

-- COMMAND ----------

CREATE TEMPORARY VIEW diamonds_csv_temp 
USING CSV
OPTIONS (path='/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv',inferSchema=True,header=True)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Create a Delta Table
-- MAGIC
-- MAGIC The SQL statement `CREATE TABLE diamonds_delta USING DELTA LOCATION '/mnt/workshop/diamonds_delta/' AS SELECT * FROM diamonds_csv_temp` is used to create a Delta Lake table in SparkSQL. 
-- MAGIC
-- MAGIC Here's a breakdown of the statement:
-- MAGIC
-- MAGIC `CREATE TABLE diamonds_delta`: This part of the statement initiates the creation of a new table named "diamonds_delta."
-- MAGIC
-- MAGIC `USING DELTA`: this clause specifies that the table will be created using the Delta Lake format. Delta Lake is a storage layer that provides ACID transactions and additional capabilities for data management.
-- MAGIC
-- MAGIC `LOCATION` '/mnt/workshop/diamonds_delta/': This clause defines the location where the Delta table will be stored. It specifies the directory path where the table's data will reside. In this case, it points to the '/mnt/workshop/diamonds_delta/' directory.
-- MAGIC
-- MAGIC `AS SELECT * FROM diamonds_csv_temp`: This part of the statement specifies the data source for the new Delta table. It instructs Spark to create the "diamonds_delta" table by copying the data from the "diamonds_csv_temp" temporary view. The "*" in "SELECT * FROM diamonds_csv_temp" means that all columns and rows from the temporary view will be included in the new Delta table.
-- MAGIC
-- MAGIC By executing this command, you are creating a new Delta Lake table named "diamonds_delta." Delta Lake tables offer transactional capabilities, schema evolution, and the ability to efficiently handle big data workloads. The data in the "diamonds_csv_temp" temporary view is being used as the source for the new Delta table, which provides advantages like data integrity, versioning, and better support for advanced data management operations compared to traditional CSV-backed tables.

-- COMMAND ----------


CREATE TABLE diamonds_delta
USING DELTA 
LOCATION '/mnt/workshop/diamonds_delta/'
AS 
SELECT * FROM diamonds_csv_temp

-- COMMAND ----------

SELECT * FROM diamonds_delta

-- COMMAND ----------

UPDATE default.diamonds_delta SET cut='Wonderful' WHERE _c0=1

-- COMMAND ----------

SELECT * FROM default.diamonds_delta WHERE _c0=1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DESCRIBE HISTORY
-- MAGIC
-- MAGIC The SQL statement `DESCRIBE HISTORY default.diamonds_delta` in SparkSQL is used to retrieve information about the version history of a Delta Lake table named "diamonds_delta." 
-- MAGIC
-- MAGIC Here's an explanation of the statement:
-- MAGIC
-- MAGIC `DESCRIBE HISTORY`: this command is specifically used with Delta Lake tables. It allows you to view the version history of a Delta table, including details about the operations and transactions that have been performed on the table over time.
-- MAGIC
-- MAGIC `default.diamonds_delta`: This part of the statement specifies the target Delta table for which you want to obtain the version history. "default.diamonds_delta" refers to the table in question, where "default" is the database name and "diamonds_delta" is the table name.
-- MAGIC
-- MAGIC When you execute `DESCRIBE HISTORY default.diamonds_delta`, you will receive a report that includes information about the different versions of the "diamonds_delta" table, such as the timestamp of each version, the operation performed, and other metadata related to the changes made to the table.

-- COMMAND ----------

DESCRIBE HISTORY default.diamonds_delta

-- COMMAND ----------

INSERT INTO TABLE default.diamonds_delta VALUES(53942,0.8,'Good','E','SI2',62.2,55,2757,5.83,5.87,3.64)

-- COMMAND ----------

DESCRIBE HISTORY default.diamonds_delta

-- COMMAND ----------

SELECT cut FROM default.diamonds_delta WHERE _c0=1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### TIME TRAVEL
-- MAGIC The  following SQL statement is used to retrieve a specific value from the "diamonds_delta" table, but it includes the "VERSION AS OF 0" clause, which specifies that the query should be executed as if it were in the initial version of the table. Here's an explanation of the statement:
-- MAGIC
-- MAGIC `SELECT cut`: This part of the statement specifies that you want to retrieve the values from the "cut" column in the "diamonds_delta" table.
-- MAGIC
-- MAGIC `FROM default.diamonds_delta`: This clause indicates that you are selecting data from the "diamonds_delta" table, which is located in the "default" database.
-- MAGIC
-- MAGIC `VERSION AS OF 0`: This is a special clause that refers to a specific version of the table. In this case, "0" represents the initial version of the table. Using "VERSION AS OF 0" means that you want to query the table as if it were in its original state, before any changes were made.
-- MAGIC
-- MAGIC `WHERE _c0=1`: This part of the statement filters the results to include only the row where the value in the "_c0" column is equal to 1.

-- COMMAND ----------

SELECT cut FROM default.diamonds_delta  VERSION AS OF 0 WHERE _c0=1

-- COMMAND ----------

DESCRIBE DETAIL default.diamonds_delta

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/mnt/workshop/diamonds_delta/'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### OPTIMIZE
-- MAGIC The SQL statement `OPTIMIZE default.diamonds_delta` in SparkSQL is used to optimize a Delta Lake table, specifically the "diamonds_delta" table in the "default" database. Here's an explanation of the statement:
-- MAGIC
-- MAGIC `OPTIMIZE`: this command is a Delta Lake-specific operation used to optimize the performance and storage of a Delta table. Delta Lake is an open-source storage layer that provides features like ACID transactions, data versioning, and advanced data management capabilities.
-- MAGIC
-- MAGIC `default.diamonds_delta`: This part of the statement specifies the Delta table that you want to optimize. In this case, "default.diamonds_delta" refers to the "diamonds_delta" table located in the "default" database.
-- MAGIC
-- MAGIC When you execute `OPTIMIZE default.diamonds_delta`, Delta Lake performs various optimizations on the specified table. These optimizations may include tasks like compaction (reducing the number of small files), data cleaning (removing unnecessary data), and other processes to enhance query performance and reduce storage costs.
-- MAGIC
-- MAGIC The "OPTIMIZE" command is especially useful in Delta Lake to maintain the health and efficiency of your data by minimizing the overhead associated with numerous small files and data history. It's a key feature that helps keep Delta Lake tables performant and cost-effective for large-scale data workloads.

-- COMMAND ----------

OPTIMIZE default.diamonds_delta 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/mnt/workshop/diamonds_delta/'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### VACUUM
-- MAGIC The SQL statement  `VACUUM default.diamonds_delta RETAIN 0 HOURS`, is a Delta Lake-specific command used to manage the retention of files and data versions in a Delta Lake table. 
-- MAGIC
-- MAGIC Here's an explanation of the statement:
-- MAGIC
-- MAGIC `VACUUM`: The "VACUUM" command is used in Delta Lake to clean up or manage the storage of data and files within a Delta table. It's particularly useful for optimizing data storage, removing unnecessary data, and managing the history of the table.
-- MAGIC
-- MAGIC `default.diamonds_delta`: This part of the statement specifies the Delta Lake table that you want to apply the VACUUM operation to. "default.diamonds_delta" refers to the table located in the "default" database.
-- MAGIC
-- MAGIC `RETAIN 0 HOURS`: The `RETAIN` clause determines how long to retain files that are no longer needed for query consistency. In this case, "0 HOURS" means that the command will remove all files and data versions that are no longer needed for query consistency immediately. Essentially, it's instructing Delta Lake to delete unnecessary data files and versions right away.

-- COMMAND ----------

VACUUM default.diamonds_delta RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled',False)

-- COMMAND ----------

VACUUM default.diamonds_delta RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/mnt/workshop/diamonds_delta/'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE A TABLE WITH ROW TO UPSERT IN THE DIAMONDS DELTA TABLE
-- MAGIC The following involves creating a new Delta Lake table named "diamonds_delta_new," populating it with specific values, and then inserting data into it. 
-- MAGIC
-- MAGIC Here's an explanation of the sequence:
-- MAGIC
-- MAGIC `CREATE TABLE default.diamonds_delta_new USING DELTA AS SELECT * FROM diamonds_delta WHERE 1=2`
-- MAGIC
-- MAGIC `CREATE TABLE default.diamonds_delta_new`: This part of the statement initiates the creation of a new Delta Lake table named "diamonds_delta_new" in the "default" database.
-- MAGIC `USING DELTA`: The "USING DELTA" clause specifies that the table will be created using the Delta Lake format, which provides features like ACID transactions and advanced data management.
-- MAGIC `AS SELECT * FROM diamonds_delta WHERE 1=2`: This part of the statement specifies that the new table will be created by selecting data from an existing Delta table named "diamonds_delta." However, the condition "WHERE 1=2" ensures that no data is actually copied from "diamonds_delta" to "diamonds_delta_new." Essentially, this part of the statement is creating an empty table with the same schema as "diamonds_delta."
-- MAGIC `INSERT INTO diamonds_delta_new VALUES (1,0.8,'Good','E','SI2',62.2,55,2757,5.83,5.87,3.6),(53943,0.8,'Good','E','SI2',62.2,55,2757,5.83,5.87,3.6)`;
-- MAGIC
-- MAGIC `INSERT INTO diamonds_delta_new`: This part of the statement specifies that you want to insert data into the "diamonds_delta_new" table.
-- MAGIC `VALUES`: This keyword indicates that you are providing specific values to be inserted into the table.
-- MAGIC The values in parentheses represent two sets of data rows. The first set contains values like (1, 0.8, 'Good', 'E', 'SI2', 62.2, 55, 2757, 5.83, 5.87, 3.6), and the second set contains similar values.

-- COMMAND ----------


CREATE TABLE default.diamonds_delta_new
USING DELTA
AS SELECT * FROM diamonds_delta WHERE 1=2;
INSERT INTO diamonds_delta_new VALUES (1,0.8,'Good','E','SI2',62.2,55,2757,5.83,5.87,3.6),(53943,0.8,'Good','E','SI2',62.2,55,2757,5.83,5.87,3.6);


-- COMMAND ----------

SELECT * FROM default.diamonds_delta_new

-- COMMAND ----------

select * from default.diamonds_delta WHERE _c0 IN (1,53943)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### `MERGE` Operation
-- MAGIC
-- MAGIC The `MERGE` operation combines data from two Delta Lake tables, "diamonds_delta" (the target) and "diamonds_delta_new" (the source). The operation specifies how to handle matching and non-matching records between the two tables. 
-- MAGIC
-- MAGIC Here's an explanation of the statement:
-- MAGIC
-- MAGIC `MERGE INTO default.diamonds_delta target`: This part of the statement initiates the MERGE operation, with "target" representing the target table, which is the destination for merged data.
-- MAGIC
-- MAGIC `USING default.diamonds_delta_new source`: The "USING" clause specifies the source table, "diamonds_delta_new," from which data will be merged into the target table.
-- MAGIC
-- MAGIC `ON source._c0 = target._c0`: This clause defines the condition for matching records between the source and target tables. In this case, records are matched based on the values in the "_c0" column.
-- MAGIC
-- MAGIC `WHEN MATCHED THEN UPDATE SET *`: This section specifies what to do when matching records are found. The "UPDATE SET *" indicates that all columns in the source record will update the corresponding columns in the target record.
-- MAGIC
-- MAGIC `WHEN NOT MATCHED THEN INSERT *`: This part of the statement specifies what to do when there are non-matching records in the source table that do not have corresponding records in the target table. The "INSERT *" clause indicates that these non-matching records will be inserted into the target table.

-- COMMAND ----------

MERGE INTO default.diamonds_delta target
USING default.diamonds_delta_new source
ON source._c0 = target._c0
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

select * from default.diamonds_delta WHERE _c0 IN (1,53943)

-- COMMAND ----------

DESCRIBE HISTORY default.diamonds_delta
