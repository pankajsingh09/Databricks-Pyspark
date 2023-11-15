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

from pyspark.sql.functions import *


# COMMAND ----------

newDf = df.select(sum("SalesAmount").alias("TotolSales"))
display(newDf)

# COMMAND ----------

newDf = df.select(round(avg("SalesAmount"),2).alias("AverageSales"))
display(newDf)

# COMMAND ----------

newDf = df.select(round(avg("SalesAmount"),2).alias("AverageSales"),round(avg("ListPrice"),2).alias("AveragePrice"),countDistinct("Country").alias("NoOfCountries") )
display(newDf)

# COMMAND ----------

newDf = df.groupBy("Country").sum("SalesAmount")
display(newDf)

# COMMAND ----------

newDf = df.groupBy("Country").sum("SalesAmount","TaxAmt")
display(newDf)

# COMMAND ----------

newDf = df.groupBy("Country").agg(sum("SalesAmount").alias("TotalSales"),round(avg("TaxAmt"),2).alias("AverageTax"))
display(newDf)

# COMMAND ----------

    