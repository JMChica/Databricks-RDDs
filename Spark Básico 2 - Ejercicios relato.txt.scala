// Databricks notebook source
// Crea un RDD con el contenido del archivo
val relatoRDD = sc.textFile("/FileStore/tables/relato.txt")

// COMMAND ----------

// Cuenta el número de líneas del archivo
relatoRDD.count()

// COMMAND ----------

// Recopila todos los elementos del RDD y los devuelve como un Array en el controlador
val relatodf = relatoRDD.toDF.collect()

// COMMAND ----------

// Aplica una función a cada elemento del RDD, en este caso printIn devería mostrar cada línea del archivo
relatodf.foreach(x=>println(x))
