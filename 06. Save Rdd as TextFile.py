# Databricks notebook source
rdd = sc.textFile('/FileStore/tables/001_Wordcount.txt')

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x : x.split(' '))

# COMMAND ----------

rdd3 = rdd2.map(lambda x : (x,1))

# COMMAND ----------

rdd4 = rdd3.reduceByKey(lambda x,y: x+y)

# COMMAND ----------

rdd4.collect()

# COMMAND ----------

rdd5 = rdd4.sortByKey()

# COMMAND ----------

rdd5.collect()

# COMMAND ----------

rdd6 = rdd4.sortBy(lambda x : x[1])

# COMMAND ----------

rdd6.collect()

# COMMAND ----------

print(rdd4.first())
print(type(rdd4.first()))

# COMMAND ----------

print(rdd6.take(3))
print(type(rdd6.take(3)))

# COMMAND ----------

rdd6.saveAsTextFile("FileStore/tables/TextFiles")

# COMMAND ----------

display(spark.read.text("/FileStore/tables/TextFiles"))

# COMMAND ----------

