# Databricks notebook source
# MAGIC %md
# MAGIC Criação de um dataframe com dados

# COMMAND ----------

#criando um dataframe com dados fixos
dados =[("Grimaldo","Oliveira","Brasileira","Professor","M",3000),
 ("Ana ","Santos","Portuguesa","Atriz","F",4000),
 
("Roberto","Carlos","Indiana","Analista","M",4000),
 ("Maria ","Santanna","Italiana","Dentista","F",6000),
 
("Jeane","Andrade","Francesa","Medica","F",7000)]
colunas=["Primeiro_Nome","Ultimo_nome","Nacionalidade","Trabalho","Genero","Salario"]
datafparquet=spark.createDataFrame(dados,colunas)
datafparquet.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Gravando o arquivo parquet

# COMMAND ----------

#criando o arquivo parquet
datafparquet.write.parquet("/FileStore/tables/parquet/pessoal.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC Subscrevendo o arquivo parquet

# COMMAND ----------

#Permite uma atualização do arquivo parquet
datafparquet.write.mode('overwrite').parquet('/FileStore/tables/parquet/pessoal.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC Lendo o arquivo parquet e guardando em um dataframe

# COMMAND ----------

#Realizando uma atualização do arquivo parquet
datafleitura = spark.read.parquet("/FileStore/tables/parquet/pessoal.parquet")
datafleitura.show()

# COMMAND ----------

#Criando uma consulta em SQL
datafleitura.createOrReplaceTempView("Tabela_Parquet")
ResultSQL = spark.sql("select * from Tabela_Parquet where salario >= 6000 ")
ResultSQL.show()


# COMMAND ----------

# MAGIC %md
# MAGIC se não quiser rodar SQL conforme script acima, pode-se criar temp view e rodar select normal

# COMMAND ----------

datafleitura.createTempView('vparquet')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vparquet

# COMMAND ----------

# MAGIC %md
# MAGIC Realizando uma consulta SQL

# COMMAND ----------

# MAGIC %md
# MAGIC **PARTICIONAMENTO**

# COMMAND ----------

# MAGIC %md
# MAGIC Particionando os dados do arquivo parquet em grupos

# COMMAND ----------

#Particionando os dados em um arquivo parquet
datafparquet.write.partitionBy("Nacionalidade","salario").mode("overwrite").parquet("/FileStore/tables/parquet/pessoal.parquet")

# COMMAND ----------

Exibindo os dados do parquet cuja a nacionalidade é brasileira

# COMMAND ----------

#Lendo o aquivo participonado do parquet
datafnacional=spark.read.parquet("/FileStore/tables/parquet/pessoal.parquet/Nacionalidade=Brasileira")
datafnacional.show(truncate=False)

# COMMAND ----------

Realizando uma pesquisa via SQL no arquivo parquet particionado

# COMMAND ----------

#Consultando diretamente o arquivo parquet particionado via SQL
spark.sql("CREATE OR REPLACE TEMPORARY VIEW Cidadao USING parquet OPTIONS (path \"/FileStore/tables/parquet/pessoal.parquet/Nacionalidade=Brasileira\")")
spark.sql("SELECT * FROM Cidadao where Ultimo_nome='Oliveira'").show()

# COMMAND ----------

