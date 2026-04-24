# Databricks notebook source
# MAGIC %md
# MAGIC id is the column with unique values for this table. This table contains information about the temperature on a certain day.
# MAGIC
# MAGIC Write a solution to find all dates' Id with higher temperatures compared to its previous dates (yesterday).
# MAGIC
# MAGIC Return the result table in any order.

# COMMAND ----------

#Pandas schema

import pandas as pd

data = [[1, '2015-01-01', 10], [2, '2015-01-02', 25], [3, '2015-01-03', 20], [4, '2015-01-04', 30]]
weather = pd.DataFrame(data, columns=['id', 'recordDate', 'temperature']).astype({'id':'Int64', 'recordDate':'datetime64[ns]', 'temperature':'Int64'})

# COMMAND ----------

# to pyspark schema

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

weather_df = spark.createDataFrame(weather)
weather_df.show(truncate=False)

# COMMAND ----------

(weather_df.alias("w1")
 .join(weather_df.alias("w2"), F.datediff(F.col("w1.recordDate"), F.col("w2.recordDate")) == 1, "inner")
 .where("w1.temperature > w2.temperature")
 .select("w1.id").show())

# COMMAND ----------

# in Spark SQL
weather_df.createOrReplaceTempView('weather')
spark.sql('''
select
    w1.id
from
    weather w1
    join weather w2 on datediff(w1.recordDate, w2.recordDate) = 1
where
    w1.temperature > w2.temperature
''').show()
