# Databricks notebook source
# MAGIC %md
# MAGIC Realizando a leitura do CSV e guardando no dataframe

# COMMAND ----------

#Leitura de arquivo CSV
dataframesp= spark.read.format("csv").option("header", 
"true").load("/FileStore/tables/arquivo/Datafiniti_Hotel_Reviews_Jun19.csv")
dataframesp.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Realizando a leitura do CSV e guardando dados no dataframe (via Scala)

# COMMAND ----------

# MAGIC %scala
# MAGIC val dataframescala = 
# MAGIC sqlContext.read.format("com.databricks.spark.csv") 
# MAGIC .option("delimiter", ",") 
# MAGIC .load("/FileStore/tables/arquivo/Datafiniti_Hotel_Reviews_Jun19.csv")
# MAGIC dataframescala.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Gravando o dataframe no formato em parquet

# COMMAND ----------

#criando o arquivo parquet
dataframesp.write.parquet("/FileStore/tables/parquet/csvparquet.parquet")

# COMMAND ----------

Realizando a leitura em parquet

# COMMAND ----------

#Realizando uma leitura do arquivo parquet
datafleitura=spark.read.parquet("/FileStore/tables/parquet/csvparquet.parquet")
datafleitura.show()

# COMMAND ----------

