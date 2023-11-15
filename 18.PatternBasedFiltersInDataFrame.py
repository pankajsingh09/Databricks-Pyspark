# Databricks notebook source
df = (spark.read.format('csv')
      .option("path", "/FileStore/tables/014_Data.csv")
      .option("header","True")
      .load()
      )

# COMMAND ----------

display(df)

# COMMAND ----------

newDf = df.filter("ProductKey== 310")
display(newDf)

# COMMAND ----------

newDf = df.where("ProductKey== 310")
display(newDf)

# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

newDf = df.where(col("ProductName").startswith("A"))
display(newDf)

# COMMAND ----------

newDf = df.where(col("ProductName").like("%a%"))
display(newDf)

# COMMAND ----------

