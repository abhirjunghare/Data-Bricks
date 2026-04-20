# Databricks notebook source
# MAGIC %md
# MAGIC In SQL, id is the primary key column for this table. Each row of this table indicates the id of a customer, their name, and the id of the customer who referred them.
# MAGIC
# MAGIC Find the names of the customer that are not referred by the customer with id = 2.
# MAGIC
# MAGIC Return the result table in any order.
# MAGIC

# COMMAND ----------

# Pandas Schema

import pandas as pd

data = [[1, 'Will', None], [2, 'Jane', None], [3, 'Alex', 2], [4, 'Bill', None], [5, 'Zack', 1], [6, 'Mark', 2]]
customer = pd.DataFrame(data, columns=['id', 'name', 'referee_id']).astype(
    {'id': 'Int64', 'name': 'object', 'referee_id': 'Int64'})

# COMMAND ----------

customer

# COMMAND ----------

#To pyspark schema

from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Spark has issues with null values when using Pandas' Int64 data type, hence creating a Spark schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("referee_id", IntegerType(), True)
])

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

customer_df = spark.createDataFrame(data, schema=schema)
customer_df.show(truncate=False)

# COMMAND ----------

# in Spark Dataframe
customer_df.filter((F.col('referee_id').isNull()) | (F.col('referee_id') != 2)).select('name').show()

# COMMAND ----------

# in Spark SQL
customer_df.createOrReplaceTempView('customer')
spark.sql('''
select name from customer where referee_id is null or referee_id!=2;
''').show()
