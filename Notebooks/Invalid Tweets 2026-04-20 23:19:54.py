# Databricks notebook source
# MAGIC %md
# MAGIC tweet_id is the primary key (column with unique values) for this table. This table contains all the tweets in a social media app.
# MAGIC
# MAGIC Write a solution to find the IDs of the invalid tweets. The tweet is invalid if the number of characters used in the content of the tweet is strictly greater than 15.
# MAGIC
# MAGIC Return the result table in any order.

# COMMAND ----------

# Pandas Schema

import pandas as pd

data = [[1, 'Vote for Biden'], [2, 'Let us make America great again!']]
tweets = pd.DataFrame(data, columns=['tweet_id', 'content']).astype({'tweet_id':'Int64', 'content':'object'})

# COMMAND ----------

# pyspark schema

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

tweets_df = spark.createDataFrame(tweets)

# COMMAND ----------

tweets_df.printSchema()

# COMMAND ----------

tweets_df.show(truncate=False)

# COMMAND ----------

# In spark Dataframe
import pyspark.sql.functions as F

tweets_df.filter(F.length(F.col('content'))>15).select('tweet_id').show()

# COMMAND ----------

#In spark SQL
tweets_df.createOrReplaceTempView('tweets')

spark.sql('''
select tweet_id from tweets where length(content)>15;
''').show()
