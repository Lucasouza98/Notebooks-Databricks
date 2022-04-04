# Databricks notebook source
# MAGIC %md
# MAGIC Aqui pode se tratar de um ETL, onde carrega a tabela bruta no DBFS puxa pelo 1° script após realiza os tratamentos necessários e após sobe novamente no DBFS tabela tratada

# COMMAND ----------

# MAGIC %python
# MAGIC cidades = spark.read.format('csv').options(header='true', 
# MAGIC inferSchema='true', 
# MAGIC delimiter=';').load('/FileStore/tables/cidades.csv')
# MAGIC display(cidades)

# COMMAND ----------

cidades.createTempView('vcidades')

# COMMAND ----------

vcidades = spark.sql("select sigla_estado, case when sigla_estado = 'SP' then 'São Paulo' else sigla_estado end as sigla_estado from vcidades").show(5)

# COMMAND ----------

vcidades.write.csv("/FileStore/tables/CSV/cidade_tratamento_gold1.csv")

# COMMAND ----------

cidades.write.csv("/FileStore/tables/CSV/cidade_tratamento_2.csv")

# COMMAND ----------

