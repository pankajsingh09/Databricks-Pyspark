# Databricks notebook source
df = (spark.read.format('csv')
      .option("path","/FileStore/tables/EmployeesNull.csv")
      .option('header', 'true')
      .option("inferschema","true")
      ).load()

# COMMAND ----------

display(df)

# COMMAND ----------

df1 = df.na.fill('NA')

# COMMAND ----------

display(df1)

# COMMAND ----------

df2 = df.na.fill(0)

# COMMAND ----------

display(df2)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

