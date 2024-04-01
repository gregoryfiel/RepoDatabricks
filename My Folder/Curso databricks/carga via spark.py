# Databricks notebook source
# DBTITLE 1,Carga de dados de um arquivo csv usando Python
clientes = spark.read.format('csv').options(header='true', inferSchema='true', delimiter=';').load('/FileStore/tables/carga/clientes_cartao.csv')
display(clientes)

# COMMAND ----------

# DBTITLE 1,Carga de dados de um arquivo csv usando SCALA
# MAGIC %scala
# MAGIC  val cliente = spark.read.format("csv")
# MAGIC  .option("header", "true")
# MAGIC  .option("inferSchema", "true")
# MAGIC  .option("delimiter", ";")
# MAGIC  .load("/FileStore/tables/carga/clientes_cartao.csv")
# MAGIC display(cliente)

# COMMAND ----------

# DBTITLE 1,Cria uma view através do SCALA
# MAGIC %scala
# MAGIC cliente.createOrReplaceTempView("dados_cliente")

# COMMAND ----------

# DBTITLE 1,Acessa a view criada anteriormente a partir de uma query SQL
# MAGIC %sql
# MAGIC select * from dados_cliente
