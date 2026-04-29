# Databricks notebook source
#pandas schema
import pandas as pd

data = [['Dog', 'Golden Retriever', 1, 5], ['Dog', 'German Shepherd', 2, 5], ['Dog', 'Mule', 200, 1],
        ['Cat', 'Shirazi', 5, 2], ['Cat', 'Siamese', 3, 3], ['Cat', 'Sphynx', 7, 4]]
queries = pd.DataFrame(data, columns=['query_name', 'result', 'position', 'rating']).astype(
    {'query_name': 'object', 'result': 'object', 'position': 'Int64', 'rating': 'Int64'})

# COMMAND ----------

#to pyspark df

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

queries_df = spark.createDataFrame(queries)
queries_df.show(truncate=False)

# COMMAND ----------

# solving in spark dataframe

queries_df \
    .groupBy('query_name') \
    .agg(
    F.round(F.avg(F.col('rating') / F.col('position')), 2).alias('quality'),
    F.round(F.avg(F.when(F.col('rating') < 3, 1).otherwise(0)) * 100, 2).alias('poor_query_percentage')
).show()

# COMMAND ----------

# solving in spark dataframe

queries_df \
    .groupBy('query_name') \
    .agg(
    F.round(F.avg(F.col('rating') / F.col('position')), 2).alias('quality'),
    F.round(F.avg(F.when(F.col('rating') < 3, 1).otherwise(0)) * 100, 2).alias('poor_query_percentage')
).show()
