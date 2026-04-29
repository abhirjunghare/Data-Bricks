# Databricks notebook source
# MAGIC %md
# MAGIC id is the primary key (column with unique values) for this table. Each row contains information about the name of a movie, its genre, and its rating. rating is a 2 decimal places float in the range [0, 10]
# MAGIC
# MAGIC Write a solution to report the movies with an odd-numbered ID and a description that is not "boring".
# MAGIC
# MAGIC Return the result table ordered by rating in descending order.
# MAGIC
# MAGIC The result format is in the following example.

# COMMAND ----------

# Pandas schema

import pandas as pd

data = [[1, 'War', 'great 3D', 8.9], [2, 'Science', 'fiction', 8.5], [3, 'irish', 'boring', 6.2],
        [4, 'Ice song', 'Fantacy', 8.6], [5, 'House card', 'Interesting', 9.1]]
cinema = pd.DataFrame(data, columns=['id', 'movie', 'description', 'rating']).astype(
    {'id': 'Int64', 'movie': 'object', 'description': 'object', 'rating': 'Float64'})


# COMMAND ----------


#to pyspark df

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

cinema_df = spark.createDataFrame(cinema)
cinema_df.show(truncate=False)

# COMMAND ----------

# spark df solution
import pyspark.sql.functions as F

cinema_df.\
    filter(~(F.col('description')=='boring') & (F.col('id')%2!=0))\
    .orderBy(F.col('rating').desc())\
    .show()

# COMMAND ----------

# spark SQL solution

cinema_df.createOrReplaceTempView('cinema')

spark.sql('''
select *
from cinema
where description!='boring' and id%2!=0
order by rating desc;
''').show()
