# Databricks notebook source
# DBTITLE 1,Lista de arquivos Json que estão armazenados no DBFS
# MAGIC %fs ls /databricks-datasets/structured-streaming/events/

# COMMAND ----------

# DBTITLE 1,Exibindo um arquivo Json com as informações
# MAGIC %fs head dbfs:/databricks-datasets/structured-streaming/events/file-1.json

# COMMAND ----------

# DBTITLE 1,Carregando 1 arquivo Json para o dataframe
# Lendo 1 arquivo JSON
dataf = spark.read.json("dbfs:/databricks-datasets/structured-streaming/events/file-1.json")
dataf.printSchema()
dataf.show()

# COMMAND ----------

# DBTITLE 1,Carregando 2 arquivos Json para o dataframe
#Lendo 2 arquivos JSON
dataf2 = spark.read.json(['dbfs:/databricks-datasets/structured-streaming/events/file-1.json','dbfs:/databricks-datasets/structured-streaming/events/file-2.json'])
dataf2.show()

# COMMAND ----------

# DBTITLE 1,Carregando TODOS os arquivos Json para o dataframe
#Lendo todos os arquivos JSON
dataf3 = spark.read.json("dbfs:/databricks-datasets/structured-streaming/events/*.json")
dataf3.show()

# COMMAND ----------

# DBTITLE 1,Unificando todos os arquivos que foram guardados no dataframe dataf3 para um novo arquivo JSON
#Gravação dos dados que estão no dataframe para JSON em um único arquivo
dataf3.write.json("/FileStore/tables/JSON/eventos.json")

# COMMAND ----------

# DBTITLE 1,Criação de uma tabela para executar SQL
spark.sql("CREATE OR REPLACE TEMPORARY VIEW view_evento USING json OPTIONS" +  " (path '/FileStore/tables/JSON/eventos.json')")
spark.sql("select action from view_evento").show()
