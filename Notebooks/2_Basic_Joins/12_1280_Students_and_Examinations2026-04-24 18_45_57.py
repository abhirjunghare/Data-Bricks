# Databricks notebook source
# MAGIC %md
# MAGIC There is no primary key (column with unique values) for this table. It may contain duplicates. Each student from the Students table takes every course from the Subjects table. Each row of this table indicates that a student with ID student_id attended the exam of subject_name.
# MAGIC
# MAGIC Write a solution to find the number of times each student attended each exam.
# MAGIC
# MAGIC Return the result table ordered by student_id and subject_name.

# COMMAND ----------

# Pandas Schema

import pandas as pd

data = [[1, 'Alice'], [2, 'Bob'], [13, 'John'], [6, 'Alex']]
students = pd.DataFrame(data, columns=['student_id', 'student_name']).astype(
    {'student_id': 'Int64', 'student_name': 'object'})
data = [['Math'], ['Physics'], ['Programming']]
subjects = pd.DataFrame(data, columns=['subject_name']).astype({'subject_name': 'object'})
data = [[1, 'Math'], [1, 'Physics'], [1, 'Programming'], [2, 'Programming'], [1, 'Physics'], [1, 'Math'], [13, 'Math'],
        [13, 'Programming'], [13, 'Physics'], [2, 'Math'], [1, 'Math']]
examinations = pd.DataFrame(data, columns=['student_id', 'subject_name']).astype(
    {'student_id': 'Int64', 'subject_name': 'object'})

# COMMAND ----------

# Pyspark conversion

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

students_df = spark.createDataFrame(students)
subjects_df = spark.createDataFrame(subjects)
examinations_df = spark.createDataFrame(examinations)

# COMMAND ----------

students_df.show()

# COMMAND ----------

subjects_df.show()

# COMMAND ----------

examinations_df.show()

# COMMAND ----------

# In spark Dataframe
from pyspark.sql import functions as F

students_subjects_df = students_df \
    .join(subjects_df)

examinations_df_renamed = examinations_df.withColumnsRenamed({'student_id': 'st_id', 'subject_name': 'sub_name'})

students_subjects_df \
    .join(examinations_df_renamed
          , on=((examinations_df_renamed['st_id'] == students_subjects_df['student_id'])
                &
                (examinations_df_renamed['sub_name'] == students_subjects_df['subject_name']))
          , how='left')\
    .withColumn('exam_count',F.when(F.isnull('st_id'),0).otherwise(1))\
    .groupBy('student_id','student_name','subject_name')\
    .agg(F.sum('exam_count').alias('attended_exams'))\
    .orderBy(F.col('student_id').asc(), F.col('student_name').asc()) \
    .show()

# COMMAND ----------

# In spark SQL

students_df.createOrReplaceTempView('students')
subjects_df.createOrReplaceTempView('subjects')
examinations_df.createOrReplaceTempView('examinations')

spark.sql('''
with students_subjects as (
    select * from students
    join subjects 
)
select 
    students_subjects.student_id,
    students_subjects.student_name,
    students_subjects.subject_name,
    sum(
    case when examinations.student_id is null then 0
    else 1 end
    ) as attended_exams
from students_subjects
left join examinations 
on students_subjects.student_id = examinations.student_id and students_subjects.subject_name = examinations.subject_name
group by students_subjects.student_id, students_subjects.student_name, students_subjects.subject_name
order by student_id, subject_name;
''').show()
