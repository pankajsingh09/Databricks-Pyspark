# Databricks notebook source
df = (spark.read.format('csv')
      .option("path","/FileStore/tables/EmployeesNew.csv")
      .option("header","True")
      ).load()

# COMMAND ----------

display(df)

# COMMAND ----------

df1 = df.distinct()
display(df1)

# COMMAND ----------

df2 = df.dropDuplicates()
display(df2)

# COMMAND ----------

df2 = df.dropDuplicates(["City"])
display(df2)

# COMMAND ----------

df2 = df.dropDuplicates(["City","id"])
display(df2)

# COMMAND ----------

df2 = df.select('City').distinct()
display(df2)

# COMMAND ----------

