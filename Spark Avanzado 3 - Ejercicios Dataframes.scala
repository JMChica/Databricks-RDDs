// Databricks notebook source
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

// COMMAND ----------

// Crea un dataframe para cada archivo csv
var googleplaystoreDF = spark.read.option("header", "true").csv("/FileStore/tables/googleplaystore.csv")
var userreviewDF = spark.read.option("header", "true").csv("/FileStore/tables/googleplaystore_user_reviews.csv")

// COMMAND ----------

// Elimina la App “Life Made WI-FI Touchscreen Photo Frame”
val nombreAppEliminar = "Life Made WI-FI Touchscreen Photo Frame"
googleplaystoreDF = googleplaystoreDF.filter(col("App") =!= nombreAppEliminar)

// COMMAND ----------

// Sustituye los valores NaN en la columna rating por "Unknown"
googleplaystoreDF = googleplaystoreDF.na.fill("Unknown", Seq("Rating"))

// COMMAND ----------

// Sustituye los valores NaN en la columna type por "Unknown"
googleplaystoreDF = googleplaystoreDF.na.fill("Unknown", Seq("Type"))

// COMMAND ----------

// Crea una nueva columna que nos indica si las características varían según el dispositivo en base a los datos de la columna "Android ver"
googleplaystoreDF = googleplaystoreDF.withColumn(
  "CaracteristicasSegunDispositivo",
  when(col("Android ver") === "Varies with device", true).otherwise(false)
)

// COMMAND ----------

// En la columna "Installs" los números aparecen con miles separados en comas y acabados en +, por lo que se elimina la coma y se usa una expresión regular para obtener los enteros y castear los datos a Int
googleplaystoreDF = googleplaystoreDF.withColumn("Installs", regexp_extract(translate(col("Installs"), ",", ""), "\\d+", 0).cast("Int"))

// Crea una nueva columna llamada Frec_Download a partir de los datos de la columna "Installs"
 googleplaystoreDF = googleplaystoreDF.withColumn(
  "Frec_Download", 
  when(col("Installs") < 50000, "Baja")
  .when(col("Installs") >= 50000 && col("Installs") < 1000000, "Media")
  .when(col("Installs") >= 1000000 && col("Installs") < 50000000, "Alta")
  .when(col("Installs") >= 50000000, "Muy Alta")
  .otherwise("Unknown")
 )

// COMMAND ----------

// Muestra aquellas aplicaciones que tengan una frecuencia de descarga muy alta y una valoración mayor a 4.5
googleplaystoreDF.filter(col("Frec_Download") === "Muy Alta" && col("Rating") > 4.5).show(Int.MaxValue)

// COMMAND ----------

// Muestra aquellas aplicaciones que tengan una frecuencia de descarga muy alta y coste gratuito
googleplaystoreDF.filter(col("Frec_Download") === "Muy Alta" && col("Type") === "Free").show(Int.MaxValue)

// COMMAND ----------

// En la columna "Price" los datos aparecen como 0 (cuando son gratis) o $4.35 (por ejemplo), por lo que se elimina el símbolo del $ y tambien se castea el 0 a double para poder compararlos
googleplaystoreDF = googleplaystoreDF.withColumn("Price",
  when(col("Price").contains("$"), 
    regexp_extract(col("Price"), "\\$(\\d+\\.\\d{2})", 1).cast("Double"))
  .otherwise(col("Price").cast("Double"))
)

// Muestra aquellas aplicaciones que tengan una frecuencia de descarga muy alta y coste gratuito
googleplaystoreDF.filter(col("Price") > 0 && col("Price") < 13).show(Int.MaxValue)


// Otra forma de hacerlo sin modificar el dataframe para que siga apareciendo el símbolo del $:
// val PriceLower13 = googleplaystoreDF.filter(
//   (when(col("Price").contains("$"), 
//     regexp_extract(col("Price"), "\\$(\\d+(\\.\\d{2})?)", 1).cast("Double"))
//   .otherwise(col("Price").cast("Double")) > 0) && 
//   (when(col("Price").contains("$"), 
//     regexp_extract(col("Price"), "\\$(\\d+(\\.\\d{2})?)", 1).cast("Double"))
//   .otherwise(col("Price").cast("Double")) < 13)
// )
// PriceLower13.show(Int.MaxValue)

// COMMAND ----------

var googleplaySample = googleplaystoreDF.sample(withReplacement = false, fraction = 0.1, seed = 123)
