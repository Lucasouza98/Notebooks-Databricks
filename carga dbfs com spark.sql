-- Databricks notebook source
-- MAGIC %md
-- MAGIC lendo arquivo que está no dbfs com spark

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clientes = spark.read.format('csv').options(header='true', 
-- MAGIC inferSchema='true', 
-- MAGIC delimiter=';').load('/FileStore/tables/carga_clientecartao/clientes_cartao.csv')
-- MAGIC display(clientes)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clientes.createOrReplaceTempView('vcliente_cartao')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("select customer_age,gender from vcliente_cartao").show(5)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC abaixo rodando por SQL a view criada em python

-- COMMAND ----------

select * from vcliente_cartao

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC  val cliente = spark.read.format("csv")
-- MAGIC  .option("header", "true")
-- MAGIC  .option("inferSchema", "true")
-- MAGIC  .option("delimiter", ";")
-- MAGIC  .load("/FileStore/tables/carga_clientecartao/clientes_cartao.csv")
-- MAGIC display(cliente)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC cliente.createOrReplaceTempView("dados_cliente")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from dados_cliente

-- COMMAND ----------

