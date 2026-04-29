# Databricks notebook source
# Spark Session
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark

# COMMAND ----------

# Emp Data & Schema

emp_data = [
    ["001","101","John Doe","30","Male","50000","2015-01-01"],
    ["002","101","Jane Smith","25","Female","45000","2016-02-15"],
    ["003","102","Bob Brown","35","Male","55000","2014-05-01"],
    ["004","102","Alice Lee","28","Female","48000","2017-09-30"],
    ["005","103","Jack Chan","40","Male","60000","2013-04-01"],
    ["006","103","Jill Wong","32","Female","52000","2018-07-01"],
    ["007","101","James Johnson","42","Male","70000","2012-03-15"],
    ["008","102","Kate Kim","29","Female","51000","2019-10-01"],
    ["009","103","Tom Tan","33","Male","58000","2016-06-01"],
    ["010","104","Lisa Lee","27","Female","47000","2018-08-01"],
    ["011","104","David Park","38","Male","65000","2015-11-01"],
    ["012","105","Susan Chen","31","Female","54000","2017-02-15"],
    ["013","106","Brian Kim","45","Male","75000","2011-07-01"],
    ["014","107","Emily Lee","26","Female","46000","2019-01-01"],
    ["015","106","Michael Lee","37","Male","63000","2014-09-30"],
    ["016","107","Kelly Zhang","30","Female","49000","2018-04-01"],
    ["017","105","George Wang","34","Male","57000","2016-03-15"],
    ["018","104","Nancy Liu","29","Female","50000","2017-06-01"],
    ["019","103","Steven Chen","36","Male","62000","2015-08-01"],
    ["020","102","Grace Kim","32","Female","53000","2018-11-01"]
]

emp_schema = "employee_id string, department_id string, name string, age string, gender string, salary string, hire_date string"

# COMMAND ----------

# Create emp DataFrame
emp = spark.createDataFrame(data=emp_data, schema=emp_schema)

# COMMAND ----------

# Show data (ACTION)

emp.show()

# COMMAND ----------

# Check number of partitions

emp.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC It looks like you've hit a specific wall with Databricks Serverless compute.
# MAGIC
# MAGIC The error occurs because Serverless compute is designed to be highly optimized and abstracted. It restricts direct access to the RDD (Resilient Distributed Dataset) API, which is the low-level foundation of Spark. Since getNumPartitions() is an RDD method, the serverless environment blocks it to ensure the engine can manage scaling and partitioning automatically.
# MAGIC
# MAGIC If you need to check or manage how your data is distributed without using RDDs, here is how you handle it in a Serverless-compatible way:

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Check Partitions via the Spark SQL API
# MAGIC Instead of dropping down to the RDD layer, you can use a SQL expression to see how the data is distributed across your current tasks.

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id

# This shows you the partition ID for each row
emp.withColumn("partition_id", spark_partition_id()).show()

# To get the count of unique partitions currently in use:
num_partitions = emp.select(spark_partition_id()).distinct().count()
print(f"Active partitions: {num_partitions}")

# COMMAND ----------

# Write our first Transformation (EMP salary > 50000)

emp_final = emp.where("salary > 50000")

# COMMAND ----------

# MAGIC %md
# MAGIC # Write data as CSV output (ACTION)
# MAGIC
# MAGIC emp_final.write.format("csv").save("data/output/1/emp.csv")
