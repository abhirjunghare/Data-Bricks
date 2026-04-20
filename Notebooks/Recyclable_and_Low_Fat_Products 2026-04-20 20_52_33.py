# Databricks notebook source
# Pandas Schema

import pandas as pd

data = [['0', 'Y', 'N'], ['1', 'Y', 'Y'], ['2', 'N', 'Y'], ['3', 'Y', 'Y'], ['4', 'N', 'N']]
products = pd.DataFrame(data, columns=['product_id', 'low_fats', 'recyclable']).astype(
    {'product_id': 'int64', 'low_fats': 'category', 'recyclable': 'category'})

# COMMAND ----------

#converting to spark dataframe
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

products_df = spark.createDataFrame(products)
products_df.printSchema()

# COMMAND ----------

products_df.show(truncate=False)

# COMMAND ----------

# in Spark Dataframe
import pyspark.sql.functions as F

products_df.filter((F.col('low_fats') == 'Y') & (F.col('recyclable') == 'Y')).select('product_id').show()

# COMMAND ----------

products_df.createOrReplaceTempView('Products')
spark.sql('''
select product_id from products where low_fats="Y" and recyclable="Y";
''').show()

# This is modified.
