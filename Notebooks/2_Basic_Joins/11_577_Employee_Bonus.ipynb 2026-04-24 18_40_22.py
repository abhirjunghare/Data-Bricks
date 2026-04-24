# Databricks notebook source
# MAGIC %md
# MAGIC empId is the column of unique values for this table. empId is a foreign key (reference column) to empId from the Employee table. Each row of this table contains the id of an employee and their respective bonus.
# MAGIC
# MAGIC Write a solution to report the name and bonus amount of each employee with a bonus less than 1000.
# MAGIC
# MAGIC Return the result table in any order.

# COMMAND ----------

# Pandas Schema

import pandas as pd

data = [[3, 'Brad', None, 4000], [1, 'John', 3, 1000], [2, 'Dan', 3, 2000], [4, 'Thomas', 3, 4000]]
employee = pd.DataFrame(data, columns=['empId', 'name', 'supervisor', 'salary']).astype(
    {'empId': 'Int64', 'name': 'object', 'supervisor': 'Int64', 'salary': 'Int64'})
data2 = [[2, 500], [4, 2000]]
bonus = pd.DataFrame(data2, columns=['empId', 'bonus']).astype({'empId': 'Int64', 'bonus': 'Int64'})

# COMMAND ----------

# to pyspark schema

from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

schema_employee = StructType([
    StructField("empId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("supervisor", IntegerType(), True),
    StructField("salary", IntegerType(), True)
])

employee_df = spark.createDataFrame(data, schema_employee)
bonus_df = spark.createDataFrame(bonus)

# COMMAND ----------

employee_df.show(truncate=False)

# COMMAND ----------


employee_df.printSchema()

# COMMAND ----------

bonus_df.show(truncate=False)

# COMMAND ----------

# In spark Dataframe

import pyspark.sql.functions as F

employee_df.join(bonus_df, on=['empId'], how='left') \
    .filter((F.col('bonus') < 1000) | (F.col('bonus').isNull())) \
    .select('name', 'bonus').show()

# COMMAND ----------

# In spark SQL

employee_df.createOrReplaceTempView('employee')
bonus_df.createOrReplaceTempView('bonus')

spark.sql('''
select name,bonus from employee
left join bonus
on employee.empId=bonus.empId
where bonus is null or bonus<1000;
''').show()

# COMMAND ----------

# modified.
