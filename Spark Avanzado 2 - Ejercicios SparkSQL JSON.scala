// Databricks notebook source
// Crea un nuevo contexto SQL
var ssc = spark.sqlContext

// COMMAND ----------

// Importa los implicits que permiten convertir RDDs en DataFrames
import sqlContext.implicits._

// COMMAND ----------

// Carga el dataset
val zips = spark.read.format("json").load("/FileStore/tables/zips.json")

// COMMAND ----------

// Visualiza los datos
zips.show()

// COMMAND ----------

// Filtra los datos para obtener las filas cuya población es superior a 10000
zips.filter(zips("pop") > 10000).collect()

// COMMAND ----------

// Crea una view temporal
zips.createTempView("zips2")

// COMMAND ----------

// Filtra los datos para obtener las filas cuya población es superior a 10000 usando SQL
ssc.sql("select * from zips2 where pop > 10000").collect()

// COMMAND ----------

// Obtiene la ciudad con más de 100 códigos postales usando SQL
ssc.sql("select city from zips2 group by city having count(*)>100 ").show()

// COMMAND ----------

// Obtiene la población del estado de Wisconsin usando SQL
ssc.sql("select SUM(pop) as POPULATION from zips2 where state='WI'").show()

// COMMAND ----------

// Obtiene los 5 estados más poblados usando SQL
ssc.sql("select state, SUM(pop) as POPULATION from zips2 group by state order by SUM(pop) DESC LIMIT 5 ").show()
