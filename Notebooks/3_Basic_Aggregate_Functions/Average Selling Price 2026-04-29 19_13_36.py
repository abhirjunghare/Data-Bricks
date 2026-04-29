# Databricks notebook source
# pandas schema

import pandas as pd

data = [[1, '2019-02-17', '2019-02-28', 5], [1, '2019-03-01', '2019-03-22', 20], [2, '2019-02-01', '2019-02-20', 15],
        [2, '2019-02-21', '2019-03-31', 30]]
prices = pd.DataFrame(data, columns=['product_id', 'start_date', 'end_date', 'price']).astype(
    {'product_id': 'Int64', 'start_date': 'datetime64[ns]', 'end_date': 'datetime64[ns]', 'price': 'Int64'})
data = [[1, '2019-02-25', 100], [1, '2019-03-01', 15], [2, '2019-02-10', 200], [2, '2019-03-22', 30]]
units_sold = pd.DataFrame(data, columns=['product_id', 'purchase_date', 'units']).astype(
    {'product_id': 'Int64', 'purchase_date': 'datetime64[ns]', 'units': 'Int64'})

# COMMAND ----------

#to pyspark df

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

prices_df = spark.createDataFrame(prices)
prices_df.show(truncate=False)

# COMMAND ----------

units_sold_df = spark.createDataFrame(units_sold)
units_sold_df.show(truncate=False)

# COMMAND ----------

# solving in spark dataframe
import pyspark.sql.functions as F

prices_df.alias('a').join(units_sold_df,
                          on=(prices_df['product_id'] == units_sold_df['product_id'])
                             & ((prices_df['start_date'] <= units_sold_df['purchase_date'])
                                & (prices_df['end_date'] >= units_sold_df['purchase_date'])),
                          how='inner') \
    .groupBy('a.product_id')\
    .agg(F.round(F.sum(F.col('price') * F.col('units')) / F.sum('units'), 2).alias('average_price'))\
    .show()

# COMMAND ----------

# solving in Spark SQL

prices_df.createOrReplaceTempView('prices')
units_sold_df.createOrReplaceTempView('unitsSold')

spark.sql('''
select prices.product_id, round(sum(price*units)/sum(units),2) as average_price 
from prices
inner join unitssold
on prices.product_id = unitssold.product_id
and (prices.start_date<=unitssold.purchase_date and prices.end_date>=unitssold.purchase_date)
group by prices.product_id;
''').show()
