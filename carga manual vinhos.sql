-- Databricks notebook source
-- MAGIC %md
-- MAGIC tabela TAB_VINHOS foi feito upload para manual no DATA

-- COMMAND ----------

SELECT * FROM treinamento_professor_grimaldo.tab_vinhos

-- COMMAND ----------

-- MAGIC %md
-- MAGIC SELECT POR PYTHON

-- COMMAND ----------

-- MAGIC %python
-- MAGIC diamonds = spark.sql("select * from treinamento_professor_grimaldo.tab_vinhos")
-- MAGIC display(diamonds.select("*"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC PLOTANDO GRÁFICOS

-- COMMAND ----------

SELECT PAIS, SUM(PRECO) AS SOMA_PRECOS FROM treinamento_professor_grimaldo.tab_vinhos GROUP BY 1 HAVING SOMA_PRECOS IS NOT NULL ORDER BY SOMA_PRECOS

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/milena.silva.andrade20@gmail.com/cidades.csv")
-- MAGIC display(df1)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clientes = spark.read.format('csv').options(header='true', 
-- MAGIC inferSchema='true', 
-- MAGIC delimiter=';').load('dbfs:/FileStore/shared_uploads/milena.silva.andrade20@gmail.com/cidades.csv')
-- MAGIC display(clientes)

-- COMMAND ----------

