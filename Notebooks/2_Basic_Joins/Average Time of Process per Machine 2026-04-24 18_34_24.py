# Databricks notebook source
# MAGIC %md
# MAGIC The table shows the user activities for a factory website. (machine_id, process_id, activity_type) is the primary key (combination of columns with unique values) of this table. machine_id is the ID of a machine. process_id is the ID of a process running on the machine with ID machine_id. activity_type is an ENUM (category) of type ('start', 'end'). timestamp is a float representing the current time in seconds. 'start' means the machine starts the process at the given timestamp and 'end' means the machine ends the process at the given timestamp. The 'start' timestamp will always be before the 'end' timestamp for every (machine_id, process_id) pair.
# MAGIC
# MAGIC There is a factory website that has several machines each running the same number of processes. Write a solution to find the average time each machine takes to complete a process.
# MAGIC
# MAGIC The time to complete a process is the 'end' timestamp minus the 'start' timestamp. The average time is calculated by the total time to complete every process on the machine divided by the number of processes that were run.
# MAGIC
# MAGIC The resulting table should have the machine_id along with the average time as processing_time, which should be rounded to 3 decimal places.
# MAGIC
# MAGIC Return the result table in any order.

# COMMAND ----------

# Pandas Schema
import pandas as pd

data = [[0, 0, 'start', 0.712], [0, 0, 'end', 1.52], [0, 1, 'start', 3.14], [0, 1, 'end', 4.12], [1, 0, 'start', 0.55],
        [1, 0, 'end', 1.55], [1, 1, 'start', 0.43], [1, 1, 'end', 1.42], [2, 0, 'start', 4.1], [2, 0, 'end', 4.512],
        [2, 1, 'start', 2.5], [2, 1, 'end', 5]]
activity = pd.DataFrame(data, columns=['machine_id', 'process_id', 'activity_type', 'timestamp']).astype(
    {'machine_id': 'Int64', 'process_id': 'Int64', 'activity_type': 'object', 'timestamp': 'Float64'})

# COMMAND ----------

#to pyspark schema
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

activity_df = spark.createDataFrame(activity)
activity_df.show()

# COMMAND ----------

# In spark Dataframe
import pyspark.sql.functions as F

activity_df \
    .groupBy('machine_id', 'process_id') \
    .agg(F.sum(
    F.when(activity_df['activity_type'] == 'start', -1 * activity_df['timestamp']) \
        .otherwise(activity_df['timestamp'])).alias('intermediate_sum')) \
    .groupBy('machine_id') \
    .agg(F.round(F.avg('intermediate_sum'), 3).alias('processing_time')) \
    .show()

# COMMAND ----------

# In spark SQL

activity_df.createOrReplaceTempView('activity')

spark.sql('''
with tbl1 as (
select machine_id, process_id, 
    sum(case when activity_type='start' then -1*timestamp else timestamp end) as intermediate_sum 
from activity
group by machine_id, process_id
)
select machine_id, round(avg(intermediate_sum),3) as processing_time
from tbl1
group by machine_id;
''').show()
