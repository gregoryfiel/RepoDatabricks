# Databricks notebook source
# DBTITLE 1,Realizando a leitura do CSV e guardando no dataframe
#Leitura de arquivo CSV
dataframesp= spark.read.format("csv").option("header","true").load("/FileStore/tables/Datafiniti_Hotel_Reviews_Jun19.csv")
dataframesp.show()

# COMMAND ----------

# DBTITLE 1,Realizando a leitura do CSV e guardando dados no dataframe (via Scala)
# MAGIC %scala
# MAGIC val dataframescala =
# MAGIC sqlContext.read.format("com.databricks.spark.csv").option("delimiter", ",").load("/FileStore/tables/Datafiniti_Hotel_Reviews_Jun19.csv")
# MAGIC dataframescala.show()
# MAGIC

# COMMAND ----------

# DBTITLE 1,Gravando o dataframe no formato em parquet
#criando o arquivo parquet
dataframesp.write.parquet("/FileStore/tables/parquet/csvparquet.parquet")

# COMMAND ----------

# DBTITLE 1,Realizando a leitura em parquet
#Realizando uma leitura do arquivo parquet
datafleitura=spark.read.parquet("/FileStore/tables/parquet/csvparquet.parquet")
datafleitura.show()
