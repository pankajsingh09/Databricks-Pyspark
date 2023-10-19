# Databricks notebook source
df = spark.read.format("csv").option("path", "dbfs:/FileStore/tables/Data.csv").option("header", "True").load()

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

df2 = df.withColumn("Country",lit("India"))
display(df2)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df3 = df.withColumn("Tax",col("sales")* 0.05)
display(df3)

# COMMAND ----------

df3.printSchema()

# COMMAND ----------

df4 = df3.withColumn("ProductKey", col("ProductKey").cast("Integer"))
df4.printSchema()

# COMMAND ----------

