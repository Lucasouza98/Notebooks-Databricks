# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

data = [('James','Smith','M',3000), ('Anna','Rose','F',4100),
  ('Robert','Williams','M',6200)
]
columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.show()

# COMMAND ----------

df2=df.withColumn("salary", df.salary*3)
df2.show()

# COMMAND ----------

df2.createOrReplaceTempView("vDf2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vDf2

# COMMAND ----------

from pyspark.sql.functions import when
df3 = df.withColumn("gender", when(df.gender == "M","Male") \
      .when(df.gender == "F","Female") \
      .otherwise(df.gender))
df3.show()

# COMMAND ----------

df4=df.withColumn("salary",df.salary.cast("String"))
df4.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("PER")
df5=spark.sql("select firstname,gender,salary*3 as salary from PER")
df5.show()

# COMMAND ----------

df5.createOrReplaceTempView("view_dados_cliente")

# COMMAND ----------

# MAGIC %python
# MAGIC diamonds = spark.sql("select * from view_dados_cliente")
# MAGIC display(diamonds.select("*"))

# COMMAND ----------

