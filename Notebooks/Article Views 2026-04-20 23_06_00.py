# Databricks notebook source
# MAGIC %md
# MAGIC There is no primary key (column with unique values) for this table, the table may have duplicate rows. Each row of this table indicates that some viewer viewed an article (written by some author) on some date. Note that equal author_id and viewer_id indicate the same person.
# MAGIC
# MAGIC Write a solution to find all the authors that viewed at least one of their own articles.
# MAGIC
# MAGIC Return the result table sorted by id in ascending order.

# COMMAND ----------

# Pandas schema

import pandas as pd

data = [[1, 3, 5, '2019-08-01'], [1, 3, 6, '2019-08-02'], [2, 7, 7, '2019-08-01'], [2, 7, 6, '2019-08-02'], [4, 7, 1, '2019-07-22'], [3, 4, 4, '2019-07-21'], [3, 4, 4, '2019-07-21']]
views = pd.DataFrame(data, columns=['article_id', 'author_id', 'viewer_id', 'view_date']).astype({'article_id':'Int64', 'author_id':'Int64', 'viewer_id':'Int64', 'view_date':'datetime64[ns]'})

# COMMAND ----------

# To pyspark schema

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

views_df = spark.createDataFrame(views)

# COMMAND ----------

views_df.printSchema()

# COMMAND ----------


views_df.show(truncate=False)

# COMMAND ----------

# In spark Dataframe
import pyspark.sql.functions as F

views_df\
    .filter(F.col('author_id')==F.col('viewer_id'))\
    .select(F.col('author_id').alias('id'))\
    .distinct()\
    .orderBy('id')\
    .show()

# COMMAND ----------

#In spark SQL
views_df.createOrReplaceTempView('views')

spark.sql('''
select distinct author_id as id
from views
where author_id=viewer_id
order by id;
''').show()
