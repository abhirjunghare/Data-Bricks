# Databricks notebook source
# MAGIC %md
# MAGIC name is the primary key (column with unique values) for this table. Each row of this table gives information about the name of a country, the continent to which it belongs, its area, the population, and its GDP value.
# MAGIC
# MAGIC A country is big if:
# MAGIC
# MAGIC it has an area of at least three million (i.e., 3000000 km2), or it has a population of at least twenty-five million (i.e., 25000000). Write a solution to find the name, population, and area of the big countries.
# MAGIC
# MAGIC Return the result table in any order.
# MAGIC

# COMMAND ----------


# Pandas Schema
import pandas as pd

data = [['Afghanistan', 'Asia', 652230, 25500100, 20343000000], ['Albania', 'Europe', 28748, 2831741, 12960000000],
        ['Algeria', 'Africa', 2381741, 37100000, 188681000000], ['Andorra', 'Europe', 468, 78115, 3712000000],
        ['Angola', 'Africa', 1246700, 20609294, 100990000000]]
world = pd.DataFrame(data, columns=['name', 'continent', 'area', 'population', 'gdp']).astype(
    {'name': 'object', 'continent': 'object', 'area': 'Int64', 'population': 'Int64', 'gdp': 'Int64'})

# COMMAND ----------

# To pyspark Schema

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

world_df = spark.createDataFrame(world)

# COMMAND ----------

world_df.printSchema()

# COMMAND ----------

world_df.show()

# COMMAND ----------

# in Spark Dataframe
import pyspark.sql.functions as F

world_df \
    .filter((F.col('area') >= 3000000) | (F.col('population') >= 25000000)) \
    .select('name', 'population', 'area') \
    .show()

# COMMAND ----------

# in Spark SQL

world_df.createOrReplaceTempView('world')

spark.sql('''
select name, population, area
from world
where area>=3000000 or population>=25000000;
''').show()
