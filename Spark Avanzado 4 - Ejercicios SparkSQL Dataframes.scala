// Databricks notebook source
import sqlContext.implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}

// COMMAND ----------

// Crea una variable con la ruta del fichero
var ruta_datos = "/FileStore/tables/DataSetPartidos.txt"

// COMMAND ----------

// Lee el contenido del archivo en una variable
var datos = sc.textFile(ruta_datos)

// COMMAND ----------

// Crea una variable que contiene el esquema de los datos
val schemaString = "idPartido::temporada::jornada::EquipoLocal::EquipoVisitante::golesLocal::golesVisitante::fecha::timestamp"

// COMMAND ----------

// Genera el esquema basado en la variable anterior que contiene el esquema de datos
val schema =  StructType(schemaString.split("::").map(fieldName => StructField(fieldName, StringType, true)))

// COMMAND ----------

// Convierte los datos en rows en un nuevo RDD
val rowRDD = datos.map(_.split("::")).map(p => Row(p(0), p(1) , p(2) , p(3) , p(4) , p(5) , p(6) , p(7) , p(8).trim))

// COMMAND ----------

// Aplica el schema al RDD de rows en un dataframe
val partidosDF = sqlContext.createDataFrame(rowRDD, schema)

// COMMAND ----------

// Crea una view a partir del dataframe
partidosDF.createTempView("DataPartidos")

// COMMAND ----------

// Ejemplo de consulta SQL sobre la view del dataframe
val results = sqlContext.sql("SELECT temporada, jornada FROM DataPartidos")
results.show()

// COMMAND ----------

// Mapea results para mostrar por pantalla cada dato de la columna "temporada" (t(0)) precedida por name:
results.map(t => "Name: " + t(0)).collect().foreach(println)

// COMMAND ----------

// Muestra el record de goles del Oviedo en una temporada jugando como visitante
val recordOviedo = sqlContext.sql("select sum(golesVisitante) as goles, temporada from DataPartidos where equipoVisitante='Real Oviedo' group by temporada order by goles desc")
recordOviedo.take(1)

// COMMAND ----------

// Muestra las temporadas jugadas en 1ª División del Oviedo y el Sporting para poder compararlas
val temporadasOviedo = sqlContext.sql("select count(distinct(temporada)) as TemporadasPrimeraOviedo from DataPartidos where equipoLocal='Real Oviedo' ")
val temporadasSporting = sqlContext.sql("select count(distinct(temporada)) as TemporadasPrimeraSporting from DataPartidos where equipoLocal='Sporting de Gijon' ")
temporadasOviedo.show()
temporadasSporting.show()
