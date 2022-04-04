# Databricks notebook source
# MAGIC %md
# MAGIC **Lista de arquivos Json que estão armazenados no DBFS**

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/structured-streaming/events/

# COMMAND ----------

# MAGIC %md
# MAGIC **Exibindo um arquivo Json com as informações**

# COMMAND ----------

# MAGIC %fs head /databricks-datasets/structured-streaming/events/file-1.json

# COMMAND ----------

# MAGIC %md
# MAGIC **Carregando 1 arquivo Json para o dataframe**

# COMMAND ----------

# MAGIC %python
# MAGIC # Lendo 1 arquivo JSON 
# MAGIC dataf = spark.read.json("/databricks-datasets/structured-streaming/events/file-1.json")
# MAGIC dataf.printSchema()
# MAGIC dataf.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Carregando 2 arquivos Json para o dataframe**

# COMMAND ----------

# MAGIC %python
# MAGIC #Lendo 2 arquivos JSON
# MAGIC dataf2 = spark.read.json(['/databricks-datasets/structured-streaming/events/file-1.json','/databricks-datasets/structured-streaming/events/file-2.json'])
# MAGIC dataf2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Carregando TODOS os arquivos Json para o dataframe**

# COMMAND ----------

# MAGIC %python
# MAGIC #Lendo todos os arquivos JSON
# MAGIC dataf3 = spark.read.json("/databricks-datasets/structured-streaming/events/*.json")
# MAGIC dataf3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Unificando todos os arquivos que foram guardados no dataframe dataf3 para um novo arquivo JSON**

# COMMAND ----------

# MAGIC %python
# MAGIC #Gravação dos dados que estão no dataframe para JSON em um único arquivo
# MAGIC dataf3.write.json("/FileStore/tables/JSON/eventos.json")

# COMMAND ----------

# MAGIC %md
# MAGIC **Criação de uma tabela para executar SQL**

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql("CREATE OR REPLACE TEMPORARY VIEW view_evento USING json OPTIONS" + 
# MAGIC  " (path '/FileStore/tables/JSON/eventos.json')")
# MAGIC spark.sql("select action from view_evento").show()

# COMMAND ----------

