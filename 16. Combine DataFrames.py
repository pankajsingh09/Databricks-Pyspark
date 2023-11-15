# Databricks notebook source
df = (spark.read.format('csv')
      .option("path","/FileStore/tables/EmployeesNew.csv")
      .option("header","True")
      ).load()

# COMMAND ----------

display(df)

# COMMAND ----------

newdf = (spark.read.format('csv')
      .option("path","/FileStore/tables/Employees.csv")
      .option("header","True")
      ).load()

# COMMAND ----------

display(newdf)

# COMMAND ----------

df2 = df.union(newdf)
display(df2)

# COMMAND ----------

df3  = df.unionByName(newdf)

# COMMAND ----------

display(df3)

# COMMAND ----------

df4  = df.unionByName(newdf).distinct().sort("name")
display(df4)

# COMMAND ----------

