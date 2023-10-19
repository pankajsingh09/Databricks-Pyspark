# Databricks notebook source
rdd = sc.parallelize([(1,"India"),(2,"US"),(3,"Germany"),(4,"Austria")])

# COMMAND ----------

rdd.collect()

# COMMAND ----------

df = rdd.toDF()

# COMMAND ----------

df.show()

# COMMAND ----------

schema = ["Id", "Country"]

# COMMAND ----------

df1 = rdd.toDF(schema)

# COMMAND ----------

df1.show()

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

newschema = ("ID Integer, Country String")

# COMMAND ----------

df2 = rdd.toDF(newschema)

# COMMAND ----------

df2.show()
df2.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructField, StringType, StructType, LongType

# COMMAND ----------

dfSchema = StructType([
    StructField("Id", LongType(), False),
    StructField("Country", StringType(), True)
                       ])

# COMMAND ----------

df3 = rdd.toDF(dfSchema)

# COMMAND ----------

display(df3)
df3.printSchema()

# COMMAND ----------

df4 = spark.createDataFrame(rdd,dfSchema)

# COMMAND ----------

df4.show()
df4.printSchema()

# COMMAND ----------

### Creating Dataframe without RDD

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

df5 = spark.createDataFrame([
    Row(1, "India"),
    Row(2, "US"),
    Row(3, "UK"),
], dfSchema
)

# COMMAND ----------

display(df5)
df5.printSchema()

# COMMAND ----------

