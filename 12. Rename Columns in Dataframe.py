# Databricks notebook source
df = spark.read.format("csv").option("path","dbfs:/FileStore/tables/Data.csv").option("header","True").load()

# COMMAND ----------

display(df)

# COMMAND ----------

df1 = df.withColumnRenamed("ProductKey","ProductId")
display(df1)

# COMMAND ----------

df1 = df.selectExpr("ProductKey as ProductId")
display(df1)

# COMMAND ----------

## selectExpr only bring the listed columns

# COMMAND ----------

df1 = df.selectExpr("ProductKey as ProductId","ProducName as ProductName","sales")
display(df1)

# COMMAND ----------

columns = list(map(lambda x : x.upper(),df.columns))

# COMMAND ----------

print(columns)

# COMMAND ----------

df2  = df.toDF(*columns)
display(df2)

# COMMAND ----------

