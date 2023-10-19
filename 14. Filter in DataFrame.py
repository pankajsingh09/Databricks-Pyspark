# Databricks notebook source
df = (spark.read.format('csv')
      .option("path", "/FileStore/tables/014_Data.csv")
      .option("header","True")
      .load()
      )

# COMMAND ----------

display(df)

# COMMAND ----------

df1 = df.filter("ProductKey = 310")
display(df1)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df1 = df.filter( col("color") == "Black")
display(df1)

# COMMAND ----------

df1 = df.filter( col("SalesAmount").between(2000,5000))
display(df1)

# COMMAND ----------

df2 = df.filter( "ProductKey = 310 and Color = 'Red'")
display(df2)

# COMMAND ----------

