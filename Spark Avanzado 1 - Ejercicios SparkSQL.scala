// Databricks notebook source
import org.apache.spark.sql._

// COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS prueba")

// COMMAND ----------

spark.sql("USE prueba")

// COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS tabla_prueba (id INT, nombre STRING, edad INT)")
spark.sql("SHOW TABLES").show()

// COMMAND ----------

val query = spark.sql("SELECT * FROM tabla_prueba")
query.createTempView("vistaDeprueba")
spark.sql("SHOW TABLES").show()

// COMMAND ----------

val query = spark.sql("SELECT * FROM tabla_prueba")
query.createGlobalTempView("vistaGlobalDePrueba")
spark.sql("SHOW TABLES").show()
