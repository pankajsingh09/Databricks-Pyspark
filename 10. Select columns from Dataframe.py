# Databricks notebook source
df = spark.read.csv("/FileStore/tables/Products.csv",header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df1 = df.select("ProductKey","Country")

# COMMAND ----------

display(df1)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df2 = df.select(col("Country"))

# COMMAND ----------

display(df2)

# COMMAND ----------

df3 = df.selectExpr("Country","TaxAmt")

# COMMAND ----------

display(df3)

# COMMAND ----------

df3 = df.selectExpr("Country","TaxAmt","TaxAmt *10")
display(df3)

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

df4 = df.select("Country","TaxAmt",expr("TaxAmt *10"))
display(df4)

# COMMAND ----------

