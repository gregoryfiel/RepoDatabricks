# Databricks notebook source
# DBTITLE 1,Particionando os dados do arquivo parquet em grupos
#Particionando os dados em um arquivo parquet
datafparquet.write.partitionBy("Nacionalidade","salario").mode("overwrite").parquet("/FileStore/tables/parquet/pessoal.parquet")


# COMMAND ----------

# DBTITLE 1,Exibindo os dados do parquet cuja a nacionalidade é brasileira
#Lendo o aquivo participonado do parquet
datafnacional=spark.read.parquet("/FileStore/tables/parquet/pessoal.parquet/Nacionalidade=Brasileira")
datafnacional.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Realizando uma pesquisa via SQL no arquivo parquet particionado
#Consultando diretamente o arquivo parquet particionado via
SQL
spark.sql("CREATE OR REPLACE TEMPORARY VIEW Cidadao USING
parquet OPTIONS (path\"/FileStore/tables/parquet/pessoal.parquet/Nacionalidade=Brasileira\")")
spark.sql("SELECT * FROM Cidadao whereUltimo_nome='Oliveira'").show()
