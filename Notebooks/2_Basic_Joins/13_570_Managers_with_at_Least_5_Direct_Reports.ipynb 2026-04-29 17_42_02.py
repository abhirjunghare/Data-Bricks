# Databricks notebook source
# MAGIC %md
# MAGIC id is the primary key (column with unique values) for this table. Each row of this table indicates the name of an employee, their department, and the id of their manager. If managerId is null, then the employee does not have a manager. No employee will be the manager of themself.
# MAGIC
# MAGIC Write a solution to find managers with at least five direct reports.
# MAGIC
# MAGIC Return the result table in any order.
# MAGIC
# MAGIC The result format is in the following example.

# COMMAND ----------

# Pandas schema

import pandas as pd

data = [[101, 'John', 'A', None], [102, 'Dan', 'A', 101], [103, 'James', 'A', 101], [104, 'Amy', 'A', 101], [105, 'Anne', 'A', 101], [106, 'Ron', 'B', 101]]
employee = pd.DataFrame(data, columns=['id', 'name', 'department', 'managerId']).astype({'id':'Int64', 'name':'object', 'department':'object', 'managerId':'Int64'})

# COMMAND ----------

# to pyspark schema

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *


schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("managerId", IntegerType(), True)
])

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

employee_df = spark.createDataFrame(data,schema=schema)
employee_df.show(truncate=False)

# COMMAND ----------

# in Spark Dataframe

employee_df.select('name').filter(F.col('id').isin(
        list(employee_df.select('managerId').groupBy('managerId').agg(F.count(F.col('managerId')).alias('count')).filter(
        F.col('count') >= 5).select('managerId').rdd.map(lambda x:x[0]).collect())
)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC You are running into this error because you are trying to use .rdd.map().collect() to create a list for a filter. In Databricks Serverless, dropping down to the RDD layer is restricted to ensure high-performance execution.
# MAGIC
# MAGIC Beyond the error, the code you are using is a bit "expensive" because it pulls data out of Spark's memory and into the driver's memory as a Python list, which can crash your session if the list is large.
# MAGIC
# MAGIC The Solution: Use a Join (The "Spark Way")
# MAGIC Instead of converting the IDs to a list, you should treat the managers as a second DataFrame and perform a Semi Join. This keeps all the processing inside Spark's optimized engine and stays fully compatible with Serverless compute.

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Identify the managers with 5+ direct reports
managers_to_include = employee_df.groupBy("managerId") \
    .agg(F.count("managerId").alias("report_count")) \
    .filter(F.col("report_count") >= 5)

# 2. Join the original DF with this list on the ID column
# A "left_semi" join only returns rows from the left table 
# that have a match in the right table.
result_df = employee_df.join(
    managers_to_include, 
    employee_df.id == managers_to_include.managerId, 
    "left_semi"
)

result_df.select("name").show()

# COMMAND ----------

# in Spark SQL
employee_df.createOrReplaceTempView('employee')
spark.sql('''
select name
from employee
where id in (select managerId from employee group by managerId having count(managerId)>=5)
''').show()
