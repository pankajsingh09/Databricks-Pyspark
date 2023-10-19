# Databricks notebook source
df = spark.read.csv("/FileStore/tables/Data.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

df2 = spark.read.csv("/FileStore/tables/Data.csv",header = True)

# COMMAND ----------

display(df2)

# COMMAND ----------

df.printSchema()


# COMMAND ----------

df3 = spark.read.csv("/FileStore/tables/Data.csv",header = True,inferSchema= True)

# COMMAND ----------

df3.printSchema()

# COMMAND ----------

df4 = spark.read.format('csv').option("path","/FileStore/tables/Data.csv").option("header","True").load()

# COMMAND ----------

display(df4)

# COMMAND ----------

df5 = (spark.read.format('csv')
       .option("path","/FileStore/tables/Data.csv")
       .option("header","True").load())

# COMMAND ----------

display(df5)

# COMMAND ----------

df5.write.save("/FileStore/tables/Files")

# COMMAND ----------

df5.write.format("CSV").option("path","/FileStore/tables/Files1").save()

# COMMAND ----------

df5.write.format("CSV").option("path","/FileStore/tables/Files1").mode("Overwrite").save()

# COMMAND ----------

df5.write.format("CSV").option("path","/FileStore/tables/Files1").mode("Append").save()

# COMMAND ----------

df5.write.format("CSV").option("path","/FileStore/tables/Files1").mode("Error").save()

# COMMAND ----------

df5.write.format("CSV").option("path","/FileStore/tables/Files1").mode("Ignore").save()

# COMMAND ----------

