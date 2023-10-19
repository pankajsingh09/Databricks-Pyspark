# Databricks notebook source
df = spark.read.format("csv").option("path", "/FileStore/tables/014_Data.csv").option("header", "True").load()

# COMMAND ----------

display(df)

# COMMAND ----------

df1 = df.sort("ProductKey")
display(df1)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df2 = df1.sort(col("ProductKey").desc())
display(df2)

# COMMAND ----------

df3 = df.sort(col("Color").asc(),col("TaxAmt").desc())
display(df3)


# COMMAND ----------

    