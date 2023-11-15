# Databricks notebook source
df = (spark.read.format('csv')
      .option("path", "/FileStore/tables/014_Data.csv")
      .option("header","True")
      .load()
      )

# COMMAND ----------

display(df)

# COMMAND ----------

df.show(5)

# COMMAND ----------

df.take(5)

# COMMAND ----------

type(df.take(5))

# COMMAND ----------

x = sc.parallelize(df.take(5))

# COMMAND ----------

newDf = x.toDF()

# COMMAND ----------

display(newDf)

# COMMAND ----------

newDf.head(2)

# COMMAND ----------

newDf.tail(1)

# COMMAND ----------

