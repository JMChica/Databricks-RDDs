// Databricks notebook source
// Crea un RDD con los pares ID y el número de veces que se repite, teniendo en cuenta que el campo ID es el tercer elemento de cada línea
val logs = sc.textFile("FileStore/tables/weblogs/*")
val userreqs = logs.map(line => line.split(' ')).map(words => (words(2),1)).reduceByKey((v1,v2) => v1 + v2)


// COMMAND ----------

// Crea un nuevo RDD con la key y valor intercambiados con la función swap
val swapped=userreqs.map(field => field.swap)
swapped.take(10)

// COMMAND ----------

// Se utiliza sortByKey(false) para ordenar el RDD en según sus claves de forma descendente y después se vuelve a usar swap para visualizar el RDD en su formato original con los 10 usuarios que más aparecen
swapped.sortByKey(false).map(field => field.swap).take(10).foreach(println)


// COMMAND ----------

// Crea un RDD formado por pares en los que el tercer elemento del array es la key y el primer elemento es el valor, además se agrupan los valores por su key
val userips = logs.map(line => line.split(' ')).map(words => (words(2),words(0))).groupByKey()
userips.take(10)


// COMMAND ----------

// Cambia el formato con el que aparece por pantalla el array anterior
userips.take(10).foreach { case (userID, ips) =>
  println(s"ID: $userID")
  println("IPS:")
  ips.foreach(ip => println(ip))
  println()
}

// COMMAND ----------

// Crea un RDD a partir de un archivo csv en el que cada dato se separa por "," y crea pares en los que la key es el ID (primer elemento del array) y el valor es el array completo
var accounts = sc.textFile("/FileStore/tables/accounts.csv").map(line => line.split(',')).map(account => (account(0),account))

// COMMAND ----------

// Crea un RDD que es un JOIN entre otros dos RDDS, userreqs añadiría el número de veces que se repite cada ID (número de visitas)
var accounthits = accounts.join(userreqs)
accounthits.take(1)

// COMMAND ----------

// Bucle para que muestre por pantalla el userID, número de visitas, nombre y apellido
for (pair <- accounthits.take(10)) {println(pair._1,pair._2._2, pair._2._1(3),pair._2._1(4))}

// COMMAND ----------

// Crea un RDD a partir de accounts.csv pero con el código postal como clave (noveno campo del fichero)
var accountsByPCode = sc.textFile("/FileStore/tables/accounts.csv").map(_.split(',')).keyBy(_(8))

// COMMAND ----------

// Crea un nuevo RDD de pares con el código postal como clave y una lista con el nombre y apellido de ese código postal como el valor
var namesByPCode = accountsByPCode.mapValues(values => values(4) + ',' + values(3)).groupByKey()

// COMMAND ----------

// Ordena los datos por código postal y muestra los 10 primeros con los todos los nombres de cada uno en un formato concreto
namesByPCode.sortByKey().take(10).foreach{
  case(x,y) => println ("---" + x)
  y.foreach(println)};

// Otra forma de obtener el mismo resultado
// namesByPCode.sortByKey().take(10).foreach{
//  x => println ("---" + x._1)
//  x._2.foreach(println)};


// COMMAND ----------

// Crea un RDD con los ficheros de shakespeare
val shakespeare = sc.textFile("/FileStore/tables/shakespeare/*")

// Crea un nuevo RDD con cada elemento separado por espacios
val shakespeare2 = shakespeare.flatMap(line => line.split(" "))

// Crea un nuevo RDD mapeando cada palabra como key y valor 1 para hacer un reduceByKey y obtener el número de veces que aparece cada palabra como valor
val shakespeare3 = shakespeare2.map(word => (word,1)).reduceByKey(_+_)

// Crea un nuevo RDD en el que primero se invierten key y valor para poder ordenar los pares según el número de veces que aparece cada palabra, al final se vuelve a hacer swap para obtener los pares con el formato original
val shakespeare4 = shakespeare3.map(word => word.swap).sortByKey(false).map(_.swap)

shakespeare4.take(5)

// COMMAND ----------

// Carga los archivos de texto de la carpeta "shakespeare" en un RDD
val logs = sc.textFile("/FileStore/tables/shakespeare/*")

// Reemplaza todos los caracteres que no sean letras con un espacio en blanco en cada línea del RDD.
val logs2 = logs.map(line => line.replaceAll("[^a-zA-Z]+", " "))

// Divide cada línea en palabras utilizando el espacio en blanco como separador y aplana las listas de palabras resultantes.
val logs3 = logs2.flatMap(line => line.split(" "))

// Convierte todas las palabras a minúsculas.
val logs4 = logs3.map(word => word.toLowerCase)

// Carga el csv "stop-word-list.csv" en un RDD.
val stopwords = sc.textFile("/FileStore/tables/stop_word_list.csv")

// Divide cada línea del RDD de stopwords por comas y aplana las listas resultantes.
val stopwords2 = stopwords.flatMap(line => line.split(","))

// Elimina los espacios en blanco de las palabras en el RDD de stopwords.
val stopwords3 = stopwords2.map(word => word.replace(" ", ""))

// Elimina las palabras vacías (espacios en blanco) del RDD de palabras.
val logs5 = logs4.subtract(sc.parallelize(Seq(" ")))

// Elimina las stopwords del RDD de palabras.
val logs6 = logs5.subtract(stopwords3)

// Mapea cada palabra a un par (palabra, 1) y luego reduce por clave (palabra) sumando los valores (1) para contar las ocurrencias de cada palabra.
val logs7 = logs6.map(word => (word, 1)).reduceByKey(_+_)

// Intercambia la clave y el valor en cada par (palabra, frecuencia) y ordena por clave en orden descendente.
val logs8 = logs7.map(word => word.swap).sortByKey(false).map(value => value.swap)

// Filtra las palabras para eliminar aquellas que tienen una longitud de 1 (generalmente caracteres no deseados).
val logs9 = logs8.filter(word => word._1.size != 1)

// Filtra las palabras para eliminar aquellas que tienen una longitud de 0.
val logs10 = logs9.filter(word => word._1.size != 0)

// Toma las primeras 20 palabras resultantes y las imprime.
logs10.take(20).foreach(println)

// COMMAND ----------

// Carga el archivo de texto "counts" en un RDD
val counts = sc.textFile("/FileStore/tables/counts")

// Divide cada línea por tabuladores
val counts2 = counts.flatMap(line => line.split("\t")) 

// Filtra palabras vacías
val counts3 = counts2.filter(word => !word.isEmpty) 

// Convierte a minúsculas
val counts4 = counts3.map(word => word.toLowerCase) 

// Crea pares clave-valor (letra inicial, 1)
val counts5 = counts4.map(word => (word.charAt(0), 1)) 

// Realiza el conteo sumando los valores
val counts6 = counts5.reduceByKey(_ + _) 

// Muestra por pantalla todos los pares (letra incial, total de veces que aparece) que forman el RDD
counts6.collect().foreach(println)
