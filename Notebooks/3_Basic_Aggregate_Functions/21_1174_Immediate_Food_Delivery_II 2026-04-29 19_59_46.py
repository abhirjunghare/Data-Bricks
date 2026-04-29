# Databricks notebook source
#Pandas schema

import pandas as pd

data = [[1, 1, '2019-08-01', '2019-08-02'], [2, 2, '2019-08-02', '2019-08-02'], [3, 1, '2019-08-11', '2019-08-12'],
        [4, 3, '2019-08-24', '2019-08-24'], [5, 3, '2019-08-21', '2019-08-22'], [6, 2, '2019-08-11', '2019-08-13'],
        [7, 4, '2019-08-09', '2019-08-09']]
delivery = pd.DataFrame(data,
                        columns=['delivery_id', 'customer_id', 'order_date', 'customer_pref_delivery_date']).astype(
    {'delivery_id': 'Int64', 'customer_id': 'Int64', 'order_date': 'datetime64[ns]',
     'customer_pref_delivery_date': 'datetime64[ns]'})

# COMMAND ----------

# to spark dataframe

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

delivery_df = spark.createDataFrame(delivery)
delivery_df.show(truncate=False)

# COMMAND ----------

# solving in spark dataframe
from pyspark.sql import Window
import pyspark.sql.functions as F

delivery_df \
    .withColumn('rn', F.row_number().over(Window.partitionBy('customer_id').orderBy('order_date'))) \
    .filter(F.col('rn') == 1) \
    .agg(
    F.round(100*
        F.sum(F.when(F.col('order_date') == F.col('customer_pref_delivery_date'), 1).otherwise(0)) / F.count('*'),
        2) \
        .alias('immediate_percentage')
) \
    .show()

# COMMAND ----------

# solving in spark SQL

delivery_df.createOrReplaceTempView('delivery')

spark.sql('''
with tbl as (
select *, row_number() over (partition by customer_id order by order_date) as rn
from delivery
)
select 
    round(
    100*sum(case when order_date=customer_pref_delivery_date then 1 else 0 end)/count(*),
    2) as immediate_percentage
from tbl
where rn = 1;
''').show()
