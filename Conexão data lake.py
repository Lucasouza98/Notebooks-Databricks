# Databricks notebook source
# MAGIC %md
# MAGIC **Integração com data lake**
# MAGIC 1° passo: Criar o datalake
# MAGIC 2° passo: Ir em access key
# MAGIC 3° pegar o nome do adls e senha

# COMMAND ----------

spark.conf.set(
"fs.azure.account.key.dlsimersaobigdata.dfs.core.windows.net",
"Fji6j8aBrB9TEeUkg2qdB/sNpLOV/sXnx+jEaJGFB8Zrn8ArlgTFIRojmsLRj+XcPBVTQqFbrzsY4cy8fmg4Tw=="
)

# COMMAND ----------

# MAGIC %md
# MAGIC Nesse processo listo o nome do container e o nome do ADLS

# COMMAND ----------

dbutils.fs.ls("abfss://conndatabricks@dlsimersaobigdata.dfs.core.windows.net/")

# COMMAND ----------

# set the data lake file location:
file_location = "abfss://conndatabricks@dlsimersaobigdata.dfs.core.windows.net/cidades.csv"

#read in the data to dataframe df
df = spark.read.format("csv").option("inferSchema","true").option("header", "true").option("delimiter",";").option("encoding","UTF-8").load(file_location)

#display dataFrame
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC conndatabricks = nome do container <br/>
# MAGIC dlsimersaobigdata = nome do data lake <br/>
# MAGIC Fji6j8aBrB9TEeUkg2qdB/sNpLOV/sXnx+jEaJGFB8Zrn8ArlgTFIRojmsLRj+XcPBVTQqFbrzsY4cy8fmg4Tw== = acess key do data lake 

# COMMAND ----------

dbutils.fs.mount(
    source = "wasbs://conndatabricks@dlsimersaobigdata.blob.core.windows.net/",
    mount_point = "/mnt/adls_test",
    extra_configs = {"fs.azure.account.key.dlsimersaobigdata.blob.core.windows.net":"Fji6j8aBrB9TEeUkg2qdB/sNpLOV/sXnx+jEaJGFB8Zrn8ArlgTFIRojmsLRj+XcPBVTQqFbrzsY4cy8fmg4Tw=="})

# COMMAND ----------

dbutils.fs.ls("/mnt/adls_test")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.unmount("/mnt/adls_test")

# COMMAND ----------

df.createOrReplaceTempView("vCidades")

# COMMAND ----------

df = spark.sql("select cod_cidade from vCidades").show(3)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vCidades

# COMMAND ----------

