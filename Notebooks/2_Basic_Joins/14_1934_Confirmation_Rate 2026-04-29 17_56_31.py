# Databricks notebook source
# Pandas schema

import pandas as pd

data = [[3, '2020-03-21 10:16:13'], [7, '2020-01-04 13:57:59'], [2, '2020-07-29 23:09:44'], [6, '2020-12-09 10:39:37']]
signups = pd.DataFrame(data, columns=['user_id', 'time_stamp']).astype(
    {'user_id': 'Int64', 'time_stamp': 'datetime64[ns]'})
data = [[3, '2021-01-06 03:30:46', 'timeout'], [3, '2021-07-14 14:00:00', 'timeout'],
        [7, '2021-06-12 11:57:29', 'confirmed'], [7, '2021-06-13 12:58:28', 'confirmed'],
        [7, '2021-06-14 13:59:27', 'confirmed'], [2, '2021-01-22 00:00:00', 'confirmed'],
        [2, '2021-02-28 23:59:59', 'timeout']]
confirmations = pd.DataFrame(data, columns=['user_id', 'time_stamp', 'action']).astype(
    {'user_id': 'Int64', 'time_stamp': 'datetime64[ns]', 'action': 'object'})

# COMMAND ----------

# to pyspark schema

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

signups_df = spark.createDataFrame(signups)
confirmations_df = spark.createDataFrame(confirmations)

# COMMAND ----------

signups_df.show()

# COMMAND ----------

confirmations_df.show()

# COMMAND ----------

# in Spark Dataframe
import pyspark.sql.functions as F

signups_df.join(confirmations_df, on=['user_id'], how='left').select('user_id', 'action') \
    .groupBy('user_id') \
    .agg(F.round(F.avg(F.when(F.col('action') == 'confirmed', 1).otherwise(0)), 2).alias('confirmation_rate')) \
    .show()

# COMMAND ----------

# in Spark SQL
signups_df.createOrReplaceTempView('signups')
confirmations_df.createOrReplaceTempView('confirmations')

spark.sql('''
select signups.user_id, round(avg(case when action='confirmed' then 1 else 0 end),2) as confirmation_rate 
from signups
left join confirmations
on signups.user_id = confirmations.user_id
group by signups.user_id
''').show()
