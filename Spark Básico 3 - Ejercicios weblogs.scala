// Databricks notebook source
// Crea una variable que contiene la ruta del fichero
val log = "/FileStore/tables/weblogs/2013_09_15-1.log"

// COMMAND ----------

// Crea un RDD con el contenido del fichero
val logs = sc.textFile(log)

// COMMAND ----------

// Crea un nuevo RDD formado solo por las lineas del RDD que contienen la cadena de caracteres ".jpg"
val jpglogs = logs.filter(x => x.contains(".jpg"))

// COMMAND ----------

// Imprime en pantalla las 5 primeras líneas del RDD anterior
jpglogs.take(5)

// COMMAND ----------

// Crea una nueva variable que devuelve el número de líneas que contienen la cadena de caracteres ".jpg"
val jpglogs2 = logs.filter(x => x.contains(".jpg")).count()

// COMMAND ----------

// Crea un array con la función map() que calcula el tamaño de las 5 primeras filas del RDD logs
logs.map(x => x.size).take(5)

// COMMAND ----------

// Imprime por pantalla cada palabra que contiene cada una de las 5 primeras filas del RDD logs usando la función split()
logs.take(5).foreach {row => 
val palabras = row.split(" ")
palabras.foreach(println)
}

// COMMAND ----------

// Mapea el contenido de logs a un nuevo RDD de arrays de palabras de cada línea
var logwords = logs.map(line => line.split(' ')) 

// COMMAND ----------

// Crea un nuevo RDD a partir del RDD logs que contiene solamente las ips de cada línea (primer elemento de cada fila)
var ips = logs.map(line => line.split(' ')(0))

//Otra forma de hacerlo:
// var ips2 = logwords.map (line => line(0))


// COMMAND ----------

// Imprime por pantalla las 5 primeras líneas de ips
ips.take(5)

// COMMAND ----------

// Visualiza el contenido de ips
// ips.collect()

// Visualiza línea por línea el contenido con foreach
val ipsdf = ips.toDF
ipsdf.foreach(x => println(x))

// COMMAND ----------

// Bucle para visualizar el contenido de las 10 primeras líneas
for (ip <- ips.take(10)) {
  println(ip)
}

// COMMAND ----------

// Crea un RDD formado por pares IP y la ID de usuario
var htmllogs = logs.filter(_.contains(".html")).map(x => (x.split(' ')(0),x.split(' ')(2)))
htmllogs.take(5).foreach(t => println(t._1 + "/" + t._2))

