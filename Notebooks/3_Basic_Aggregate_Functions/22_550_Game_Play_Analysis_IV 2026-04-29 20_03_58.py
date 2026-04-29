# Databricks notebook source
#pandas schema
import pandas as pd

data = [[1, 2, '2016-03-01', 5], [1, 2, '2016-03-02', 6], [2, 3, '2017-06-25', 1], [3, 1, '2016-03-02', 0],
        [3, 4, '2018-07-03', 5]]
activity = pd.DataFrame(data, columns=['player_id', 'device_id', 'event_date', 'games_played']).astype(
    {'player_id': 'Int64', 'device_id': 'Int64', 'event_date': 'datetime64[ns]', 'games_played': 'Int64'})

# COMMAND ----------

# to spark dataframe

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

activity_df = spark.createDataFrame(activity)
activity_df.show(truncate=False)

# COMMAND ----------

# solving in spark dataframe
from pyspark.sql import Window
import pyspark.sql.functions as F

activity_df.withColumn('next_date',
                       F.lead('event_date').over(Window.partitionBy('player_id').orderBy('event_date'))
                       ) \
    .withColumn('rn', F.row_number().over(Window.partitionBy('player_id').orderBy('event_date'))) \
    .filter(F.col('rn') == 1) \
    .agg(
    F.round(
        F.sum(F.when(F.datediff(F.col('next_date'), F.col('event_date')) == 1, 1).otherwise(0)) / F.count('player_id')
        , 2).alias('fraction')
) \
    .show()

# COMMAND ----------

# solving in Spark SQL

activity_df.createOrReplaceTempView('activity')

spark.sql('''
with tbl as (
    select 
        *,
        lead(event_date) over (partition by player_id order by event_date) as next_date,
        row_number() over (partition by player_id order by event_date) as rn
    from activity
)
select 
    round(
    sum(case when datediff(next_date,event_date)=1 then 1 else 0 end)/count(*)
    ,2) as fraction 
from tbl
where rn=1;
''').show()
