# Databricks notebook source
# Pandas schema

import pandas as pd

data = [[1, 2, 3], [1, 2, 4], [1, 3, 3], [2, 1, 1], [2, 2, 1], [2, 3, 1], [2, 4, 1]]
teacher = pd.DataFrame(data, columns=['teacher_id', 'subject_id', 'dept_id']).astype(
    {'teacher_id': 'Int64', 'subject_id': 'Int64', 'dept_id': 'Int64'})

# COMMAND ----------

#converting to spark dataframe
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

teacher_df = spark.createDataFrame(teacher)
teacher_df.show()

# COMMAND ----------

# solving in spark dataframe
import pyspark.sql.functions as F

teacher_df \
    .select('teacher_id', 'subject_id') \
    .groupBy('teacher_id') \
    .agg(F.count_distinct('subject_id').alias('cnt')) \
    .show()

# COMMAND ----------


# solving in spark SQL

teacher_df.createOrReplaceTempView('teacher')

spark.sql('''
select 
    teacher_id,
    count(distinct subject_id) as cnt 
from teacher
group by teacher_id
''').show()
