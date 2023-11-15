# Databricks notebook source
df = (spark.read.format('csv')
      .option("path", "/FileStore/tables/014_Data.csv")
      .option("header","True")
      .option("inferSchema","True")
      .load()
      )

# COMMAND ----------

display(df)

# COMMAND ----------

newDf =  df.groupBy("ProductName").pivot("Country").sum("SalesAmount")
display(newDf)

# COMMAND ----------

df2 = newDf.unpivot("ProductName",["India","Canada"],"Country","SalesAmount")
display(df2)

# COMMAND ----------

