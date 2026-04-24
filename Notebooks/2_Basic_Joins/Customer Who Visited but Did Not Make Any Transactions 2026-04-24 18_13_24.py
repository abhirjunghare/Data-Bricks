# Databricks notebook source
# MAGIC %md
# MAGIC Explanation: Customer with id = 23 visited the mall once and made one transaction during the visit with id = 12. Customer with id = 9 visited the mall once and made one transaction during the visit with id = 13. Customer with id = 30 visited the mall once and did not make any transactions. Customer with id = 54 visited the mall three times. During 2 visits they did not make any transactions, and during one visit they made 3 transactions. Customer with id = 96 visited the mall once and did not make any transactions. As we can see, users with IDs 30 and 96 visited the mall one time without making any transactions. Also, user 54 visited the mall twice and did not make any transactions.

# COMMAND ----------

#pandas schema

import pandas as pd

data = [[1, 23], [2, 9], [4, 30], [5, 54], [6, 96], [7, 54], [8, 54]]
visits = pd.DataFrame(data, columns=['visit_id', 'customer_id']).astype({'visit_id':'Int64', 'customer_id':'Int64'})
data = [[2, 5, 310], [3, 5, 300], [9, 5, 200], [12, 1, 910], [13, 2, 970]]
transactions = pd.DataFrame(data, columns=['transaction_id', 'visit_id', 'amount']).astype({'transaction_id':'Int64', 'visit_id':'Int64', 'amount':'Int64'})

# COMMAND ----------

# to pyspark schema
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

visits_df = spark.createDataFrame(visits)
visits_df.show(truncate=False)

# COMMAND ----------

transactions_df = spark.createDataFrame(transactions)
transactions_df.show(truncate=False)

# COMMAND ----------

# In spark Dataframe
import pyspark.sql.functions as f

visits_df\
    .join(transactions_df,on=['visit_id'],how='left')\
    .filter(f.col('transaction_id').isNull())\
    .groupBy('customer_id')\
    .agg(f.count('customer_id').alias('count_no_trans'))\
    .show()

# COMMAND ----------

# In spark SQL
visits_df.createOrReplaceTempView('visits')
transactions_df.createOrReplaceTempView('transactions')

spark.sql('''
select customer_id, count(customer_id) as count_no_trans
from visits 
left join transactions
on transactions.visit_id=visits.visit_id
where transaction_id is null
group by customer_id;
''').show()
