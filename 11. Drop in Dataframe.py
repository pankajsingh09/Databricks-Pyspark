# Databricks notebook source
df = spark.read.format("csv").option("path","dbfs:/FileStore/tables/Data.csv").option("header","True").load()

# COMMAND ----------

display(df)

# COMMAND ----------

df1 = df.drop("sales")

# COMMAND ----------

display(df1)

# COMMAND ----------

