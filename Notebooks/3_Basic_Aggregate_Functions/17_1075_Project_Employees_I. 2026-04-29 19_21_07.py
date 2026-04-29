# Databricks notebook source
# pandas schema

import pandas as pd

data = [[1, 1], [1, 2], [1, 3], [2, 1], [2, 4]]
project = pd.DataFrame(data, columns=['project_id', 'employee_id']).astype(
    {'project_id': 'Int64', 'employee_id': 'Int64'})
data = [[1, 'Khaled', 3], [2, 'Ali', 2], [3, 'John', 1], [4, 'Doe', 2]]
employee = pd.DataFrame(data, columns=['employee_id', 'name', 'experience_years']).astype(
    {'employee_id': 'Int64', 'name': 'object', 'experience_years': 'Int64'})

# COMMAND ----------

# to pyspark dataframe

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

project_df = spark.createDataFrame(project)
project_df.show(truncate=False)

# COMMAND ----------

employee_df = spark.createDataFrame(employee)
employee_df.show(truncate=False)

# COMMAND ----------

# solving in spark dataframe
import pyspark.sql.functions as F

project_df\
    .join(employee_df,on=['employee_id'],how='inner')\
    .groupBy('project_id').agg(F.avg('experience_years').alias('average_years'))\
    .show()

# COMMAND ----------

# solving in spark sql

employee_df.createOrReplaceTempView('employee')
project_df.createOrReplaceTempView('project')

spark.sql('''
select project_id, round(avg(experience_years),2) as average_years
from project
inner join employee
on project.employee_id=employee.employee_id
group by project_id;
''').show()
