-- Databricks notebook source
-- MAGIC %sql
-- MAGIC select pais, sum(preco) as total_vendido from treinamento_professor_grimaldo.tab_vinhos
-- MAGIC where preco > 0 
-- MAGIC group by pais
-- MAGIC order by total_vendido desc
-- MAGIC limit 10

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select pais, variante, sum(preco) as total_vendido from tab_vinhos
-- MAGIC where preco > 0 
-- MAGIC group by pais,variante
-- MAGIC order by total_vendido desc
-- MAGIC limit 10

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select pontos, preco from treinamento_professor_grimaldo.tab_vinhos

-- COMMAND ----------

