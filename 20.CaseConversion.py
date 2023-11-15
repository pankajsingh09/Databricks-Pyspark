# Databricks notebook source
df = (spark.read.format('csv')
      .option("path", "/FileStore/tables/014_Data.csv")
      .option("header","True")
      .load()
      )

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col,upper,lower,initcap

# COMMAND ----------

newDf = df.withColumn("ProductName", lower(col("ProductName")))
display(newDf)

# COMMAND ----------

newDf = df.withColumn("ProductName", upper(col("ProductName")))
display(newDf)

# COMMAND ----------

newDf = df.withColumn("ProductName", initcap(col("ProductName")))
display(newDf)

# COMMAND ----------

