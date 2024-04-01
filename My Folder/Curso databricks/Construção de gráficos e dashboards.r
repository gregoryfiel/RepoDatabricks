# Databricks notebook source
# DBTITLE 1,Script 1
## uso base interna CO2
library(ggplot2)
ggplot(data = CO2) + geom_point(mapping = aes(x = conc, y = uptake))

# COMMAND ----------

# DBTITLE 1,Script 2
# Mapa de Bolhas
# Bibliotecas
install.packages("gridExtra")
library(ggplot2)
library(dplyr)

# Carregamento dos dados
install.packages("gapminder")
library(gapminder)
data <- gapminder %>% filter(year == "2007") %>% dplyr::select(-year)

# Exibição do gráfico
data %>%
  arrange(desc(pop)) %>%
  mutate(country = factor(country, country)) %>%
  ggplot(aes(x = gdpPercap, y = lifeExp, size = pop, color = continent)) +
  geom_point(alpha = 0.7) +
  scale_size(range = c(0.5, 24), name = "População (M)") +
  labs(x = "Valor per Capita", y = "Expectativa de Vida")

# COMMAND ----------

# DBTITLE 1,Script 3
library(ggplot2)
# base interna mtcars - Violino
dados <- ggplot(mtcars, aes(factor(cyl), mpg))
dados + geom_violin()
