# Databricks notebook source
df = (spark.read.format('csv')
      .option("path", "/FileStore/tables/014_Data.csv")
      .option("header","True")
      .load()
      )

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import when,col

# COMMAND ----------

newDf = df.withColumn("Category",when(col("Color")=="Red",1).otherwise(0))

# COMMAND ----------

display(newDf)

# COMMAND ----------

df2 = df.withColumn("Category",when(col("Color")=="Red",1).when(col("Color")=="Black",2).when(col("Color")=="Silver",3).otherwise(0))
display(df2)

# COMMAND ----------

