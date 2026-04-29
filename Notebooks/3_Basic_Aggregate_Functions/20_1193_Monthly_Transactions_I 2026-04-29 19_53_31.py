# Databricks notebook source
# pandas schema

import pandas as pd

data = [[121, 'US', 'approved', 1000, '2018-12-18'], [122, 'US', 'declined', 2000, '2018-12-19'],
        [123, 'US', 'approved', 2000, '2019-01-01'], [124, 'DE', 'approved', 2000, '2019-01-07']]
transactions = pd.DataFrame(data, columns=['id', 'country', 'state', 'amount', 'trans_date']).astype(
    {'id': 'Int64', 'country': 'object', 'state': 'object', 'amount': 'Int64', 'trans_date': 'datetime64[ns]'})

# COMMAND ----------

# to spark dataframe

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

transactions_df = spark.createDataFrame(transactions)
transactions_df.show(truncate=False)

# COMMAND ----------

# solving in spark dataframe
import pyspark.sql.functions as F

transactions_df.withColumn('month', F.substring('trans_date', 0, 7)) \
    .groupBy('month', 'country') \
    .agg(
    F.count('id').alias('trans_count') \
    , F.sum(F.when(F.col('state') == 'approved', 1).otherwise(0)).alias('approved_count') \
    , F.sum('amount').alias('trans_total_amount') \
    , F.sum(F.when(F.col('state') == 'approved', F.col('amount')).otherwise(0)).alias('approved_total_amount') \
    ) \
    .show()

# COMMAND ----------

# solving in spark SQL

transactions_df.createOrReplaceTempView('transactions')

spark.sql('''
select 
    left(trans_date,7) as month,
    country,
    count(id) as trans_count,
    sum(case when state='approved' then 1 else 0 end) as approved_count,
    sum(amount) as trans_total_amount,
    sum(case when state='approved' then amount else 0 end) as approved_total_amount
from transactions
group by month, country;
''').show()
