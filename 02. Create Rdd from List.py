# Databricks notebook source
#What Is PySpark RDD?
#Resilient Distributed Datasets, often known as RDDs, are the components used in a cluster's parallel processing that run and operate across numerous nodes. Since RDDs are immutable elements, you cannot alter them after creation.

# COMMAND ----------

#RDD and Dataframe
# RDD stands for Resilient Distributed Datasets. It is Read-only partition collection of records. RDD is the fundamental data structure of Spark. It allows a programmer to perform in-memory computations

# In Dataframe, data organized into named columns. For example a table in a relational database. It is an immutable distributed collection of data. DataFrame in Spark allows developers to impose a structure onto a distributed collection of data, allowing higher-level abstraction.

# If you want to apply a map or filter to the whole dataset, use RDD
# If you want to work on an individual column or want to perform operations/calculations on a column then use Dataframe.
# for example, if you want to replace 'A' in whole data with 'B' then RDD is useful.

# rdd = rdd.map(lambda x: x.replace('A','B')
# if you want to update the data type of the column, then use Dataframe.

# dff = dff.withColumn("LastmodifiedTime_timestamp", col('LastmodifiedTime_time').cast('timestamp')
# RDD can be converted into Dataframe and vice versa. 

# COMMAND ----------

lst = [1,2,3,4,5,6,7]

# COMMAND ----------

print(lst)

# COMMAND ----------

type(lst)

# COMMAND ----------

sparksc = spark.sparkContext

# COMMAND ----------

type(sparksc)

# COMMAND ----------

print(sparksc)

# COMMAND ----------

rdd = sparksc.parallelize(lst)

# COMMAND ----------

type(rdd)

# COMMAND ----------

rdd.collect()

# COMMAND ----------

#What is the difference between transformations and actions in PySpark?

#Transformations create a new DataFrame without immediately computing the result, while PySpark actions trigger the computation of the DataFrame and return a value or perform a side effect on the data.

# here in above example rdd.collect is a action and rdd.parallelize is a transformation

# COMMAND ----------

