# Databricks notebook source
# DBTITLE 1,Gráfico de barras
# MAGIC %sql
# MAGIC select pais, sum(preco) as total_vendido from vinho
# MAGIC where preco > 0
# MAGIC group by pais
# MAGIC order by total_vendido desc
# MAGIC limit 10
# MAGIC

# COMMAND ----------

# DBTITLE 1,Gráfico de rosca
# MAGIC %sql
# MAGIC select pais, variante, sum(preco) as total_vendido from vinho
# MAGIC where preco > 0
# MAGIC group by pais,variante
# MAGIC order by total_vendido desc
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select pontos, preco from vinho
