-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Spark SQL Notebook Introduction
-- MAGIC
-- MAGIC ## Description
-- MAGIC
-- MAGIC In this guide, we will delve into SparkSQL, a powerful tool for processing and analyzing vast datasets using the Apache Spark framework. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SHOW DATABASES
-- MAGIC
-- MAGIC When you run the "SHOW DATABASES" command, SparkSQL will return a list of all the databases available within your Databricks  workspace. Each database represents a separate namespace where tables can be created and organized. This can be helpful in scenarios where you have multiple projects, datasets, or users, and you want to segregate data and tables for better organization.

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DROP TABLE
-- MAGIC When you execute the `DROP TABLE` command, you are instructing SparkSQL to permanently delete a single table along with all the data it contains. With `IF EXISTS` If the table does not exist, no action is taken, ensuring that the command doesn't result in errors. 
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS test.airport_codes;
DROP TABLE IF EXISTS test.departure_delays;
DROP TABLE IF EXISTS test.us_population;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DROP DATABASE
-- MAGIC When you execute the `DROP DATABASE` command, you are effectively deleting an entire database With `IF EXISTS` If the database does not exist, no action is taken, ensuring that the command doesn't result in errors. 
-- MAGIC

-- COMMAND ----------


DROP DATABASE IF EXISTS test

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE DATABASE 
-- MAGIC When you execute the `CREATE DATABASE` command, you are instructing SparkSQL to generate a new, empty database with the specified name. 

-- COMMAND ----------

CREATE DATABASE test;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE TABLE
-- MAGIC In SparkSQL, the `CREATE TABLE` command is used to define and create a new table in your database. This command specifies the table's name, data source, location of the data file, and additional options for table creation.
-- MAGIC
-- MAGIC Explanation:
-- MAGIC
-- MAGIC `CREATE TABLE test.departure_delays`: This part of the command defines the table name and its location within the database. In this case, it creates a table named "departure_delays" in the "test" database.
-- MAGIC
-- MAGIC `USING CSV`: The "USING" clause specifies the format of the data source. Here, "CSV" indicates that the table will be populated with data from a CSV file.
-- MAGIC
-- MAGIC `LOCATION` '/databricks-datasets/learning-spark-v2/flights/departuredelays.csv': This line points to the location of the CSV data file that will be used to populate the table. In this case, the data is expected to be in the specified file path.
-- MAGIC
-- MAGIC OPTIONS:
-- MAGIC
-- MAGIC - `HEADER=True`: This option specifies that the first row of the CSV file contains column headers.
-- MAGIC
-- MAGIC - `INFERSCHEMA=True`: When set to "True," this option tells SparkSQL to automatically infer the schema (column names and data types) from the data in the CSV file. This is a helpful feature when you want SparkSQL to determine the schema without manual specification.

-- COMMAND ----------

CREATE TABLE test.departure_delays
USING CSV
LOCATION '/databricks-datasets/learning-spark-v2/flights/departuredelays.csv'
OPTIONS (
  HEADER=True,
  INFERSCHEMA=True
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DESCRIBE TABLE
-- MAGIC In SparkSQL, the `DESCRIBE TABLE` command is used to retrieve information about the structure and characteristics of a specific table. This command provides valuable metadata about the table, including column names, data types, and other attributes.
-- MAGIC
-- MAGIC Explanation:
-- MAGIC
-- MAGIC `DESCRIBE TABLE` test.departure_delays: This command is used to describe the "departure_delays" table located in the "test" database. It retrieves information about the table's schema and characteristics.

-- COMMAND ----------

DESCRIBE TABLE test.departure_delays

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a Table Using JSON
-- MAGIC
-- MAGIC In SparkSQL, when you use the `USING JSON` clause within a `CREATE TABLE` command, you are specifying that the data source for the table is a JSON file. This indicates that the table will be populated with data from a JSON file, and it's essential to ensure that the file's content adheres to the JSON format.
-- MAGIC
-- MAGIC Explanation:
-- MAGIC
-- MAGIC `USING JSON`: The "USING" clause is used to specify the format of the data source. In this case, "JSON" indicates that the data to populate the table is expected to be in JSON format.

-- COMMAND ----------

CREATE TABLE test.us_population
USING JSON
LOCATION "/databricks-datasets/learning-spark-v2/us_population.json"

-- COMMAND ----------

DESCRIBE TABLE test.us_population

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Retrieving Data with SELECT * FROM
-- MAGIC
-- MAGIC In SparkSQL, the `SELECT * FROM` command is used to retrieve all records from a specific table. This command allows you to query and view the entire contents.

-- COMMAND ----------

SELECT * FROM test.departure_delays

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Using LIMIT to Constrain Query Results
-- MAGIC
-- MAGIC In SparkSQL, the `LIMIT` clause is utilized to restrict the number of rows returned by a query. When you specify a numerical value after "LIMIT," as in "LIMIT 5," it limits the result set to the specified number of rows. This is especially valuable when you want to retrieve only a subset of the data, such as the top N rows, for quicker data inspection or to reduce the volume of results.

-- COMMAND ----------

SELECT * FROM test.departure_delays LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Filtering Data with the WHERE Clause
-- MAGIC
-- MAGIC In SparkSQL, the `WHERE` clause is used to filter data based on specified conditions. When you include a `WHERE` clause in your query, you can define conditions that records must meet to be included in the result set.
-- MAGIC
-- MAGIC Explanation:
-- MAGIC
-- MAGIC WHERE delay > 0: This part of the query specifies the condition that each row in the "departure_delays" table must satisfy to be included in the result. Specifically, it selects rows where the "delay" column has a value greater than 0.

-- COMMAND ----------

SELECT *
FROM test.departure_delays
WHERE `delay` > 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Grouping Data with the "GROUP BY" Clause
-- MAGIC
-- MAGIC In SparkSQL, the "GROUP BY" clause is used to group rows of data based on one or more columns. When you use "GROUP BY," the query aggregates data into groups or sets based on common values in the specified columns. 
-- MAGIC
-- MAGIC Explanation:
-- MAGIC
-- MAGIC GROUP BY origin, destination: This part of the query specifies that the data should be grouped into sets based on unique combinations of values in the "origin" and "destination" columns. Rows with the same origin and destination will be grouped together.

-- COMMAND ----------

SELECT origin,destination,AVG(delay) AS avg_delay
FROM test.departure_delays
WHERE delay > 0
GROUP BY origin, destination

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Sorting Data with the ORDER BY Clause
-- MAGIC
-- MAGIC In SparkSQL, the `ORDER BY` clause is used to sort the result set based on one or more columns. When you use `ORDER BY`, you can specify the columns by which you want to sort the data and the sort order (ascending or descending). 
-- MAGIC
-- MAGIC Explanation:
-- MAGIC
-- MAGIC ORDER BY AVG(delay) DESC: This part of the query specifies that the result set should be sorted based on the `AVG(delay)` column in descending order (indicated by "DESC"). This means that the rows with the highest average delay will appear at the top of the result set.

-- COMMAND ----------

SELECT origin,destination,AVG(delay)
FROM test.departure_delays
WHERE delay > 0
GROUP BY origin, destination
ORDER BY AVG(delay) DESC

-- COMMAND ----------

-- MAGIC
-- MAGIC %md
-- MAGIC ### Concatenating Columns with the `CONCAT` Function
-- MAGIC
-- MAGIC In SparkSQL, the `CONCAT` function is used to concatenate or combine the values from one or more columns into a single column or string. This function is often used to create a new column that represents a combination of existing columns.
-- MAGIC
-- MAGIC Explanation:
-- MAGIC
-- MAGIC `CONCAT(origin,'-',destination)``: This part of the query specifies that you want to create a new column named `itinerary` by combining the values from the `origin` and `destination` columns, separated by a hyphen ('-'). The resulting column will display routes in the format "origin-destination."

-- COMMAND ----------

SELECT *, CONCAT(origin,'-',destination) AS itinerary
FROM test.departure_delays

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a New Column: "delay_in_hours"
-- MAGIC
-- MAGIC In SparkSQL, you can create a new column in your query by performing operations on existing columns.
-- MAGIC
-- MAGIC Explanation:
-- MAGIC
-- MAGIC delay/60: This part of the query calculates the delay in hours by dividing the "delay" column (which presumably represents delay in minutes) by 60. The result is a new column that represents the delay in hours.
-- MAGIC
-- MAGIC AS delay_in_hours: The "AS" keyword is used to assign a name to the new column, which is "delay_in_hours" in this case.

-- COMMAND ----------

SELECT *, delay/60 AS delay_in_hours
FROM test.departure_delays 
WHERE delay > 0

-- COMMAND ----------

CREATE TABLE test.airport_codes
USING CSV
LOCATION '/databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt'
OPTIONS (
  HEADER=True,
  INFERSCHEMA=True,
  SEP='\t'
)

-- COMMAND ----------

DESCRIBE TABLE test.airport_codes;

-- COMMAND ----------

SELECT * FROM test.airport_codes;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Joining Tables with `LEFT JOIN`
-- MAGIC
-- MAGIC In SparkSQL, a `LEFT JOIN` operation combines data from two tables based on a specified condition, retaining all records from the left (first) table and matching records from the right (second) table. This operation is used to combine data from multiple sources and create a result set that includes records from both tables, even if there are no matches in the right table.
-- MAGIC
-- MAGIC Explanation:
-- MAGIC
-- MAGIC `LEFT JOIN`` test.airport_codes ac: This part of the query specifies a `LEFT JOIN`` operation between the `departure_delays`` table (aliased as `dd``) and the `airport_codes`` table (aliased as `ac``). The `LEFT JOIN`` ensures that all records from the `departure_delays`` table are included in the result, and matching records from the `airport_codes`` table are joined based on the condition that `dd.origin` is equal to `ac.IATA.`

-- COMMAND ----------

SELECT * 
FROM test.departure_delays dd
LEFT JOIN test.airport_codes ac WHERE dd.origin=ac.IATA
