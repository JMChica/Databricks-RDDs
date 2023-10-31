# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Guarda la ruta del archiv en una variable
ruta_archivo = "/FileStore/tables/padron2.csv"

# COMMAND ----------

# Define el esquema de los datos en el csv
esquema = StructType([
    StructField("cod_distrito", IntegerType(), True),
    StructField("desc_distrito", StringType(), True),
    StructField("cod_dist_barrio", IntegerType(), True),
    StructField("desc_barrio", StringType(), True),
    StructField("cod_barrio", IntegerType(), True),
    StructField("cod_dist_seccion", IntegerType(), True),
    StructField("cod_seccion", IntegerType(), True),
    StructField("cod_edad_int", IntegerType(), True),
    StructField("espanoleshombres", IntegerType(), True),
    StructField("espanolesmujeres", IntegerType(), True),
    StructField("extranjeroshombres", IntegerType(), True),
    StructField("extranjerosmujeres", IntegerType(), True)
])

# COMMAND ----------

# Lee el csv en una variable, indicándole que no hay header, el separador es ';' y a partir del esquema definido anteriormente
padronDF =spark.read.csv(ruta_archivo, header=False, sep=';', schema=esquema)

# COMMAND ----------

# df3 = padronDF.withColumn("campoA", when(col("campoA").isNa(), lit(0)).otherwise(col("campoA")))

# COMMAND ----------

# Define una función para reemplazar los campos vacios por 0
def replace_empty_with_zero(value):
    return int(value) if value else 0

# COMMAND ----------

# Registra la función en Spark
replace_empty_with_zero_udf = udf(replace_empty_with_zero, IntegerType())

# COMMAND ----------

# Lista las columnas del csv que son númericas 
columnas_numericas = [
    "cod_distrito",
    "cod_dist_barrio",
    "cod_barrio",
    "cod_dist_seccion",
    "cod_seccion",
    "cod_edad_int",
    "espanoleshombres",
    "espanolesmujeres",
    "extranjeroshombres",
    "extranjerosmujeres"
]

# COMMAND ----------

# Bucle para aplicarle la función previamente registrada a las columnas numéricas
for col_name in columnas_numericas:
    padronDF = padronDF.withColumn(col_name, replace_empty_with_zero_udf(col(col_name)))

# COMMAND ----------

# Bucle para eliminar las comillas y espacios en blanco innecesarios del DF
for col_name in padronDF.columns:
    padronDF = padronDF.withColumn(col_name, regexp_replace(col_name, '"', ''))
    padronDF = padronDF.withColumn(col_name, trim(col_name))

# COMMAND ----------

# Las operaciones anterirores provoca que todas las columnas se conviertan en string, por lo que hay que castear las que eran enteros
for col_name in columnas_numericas:
    padronDF = padronDF.withColumn(col_name, padronDF[col_name].cast(IntegerType()))

# COMMAND ----------

# Muestra el DF para comprobar que los datos están bien
padronDF.show()

# COMMAND ----------

# Cuenta y muestra todos los barrios distintos
padronDF.select("desc_barrio").distinct().count()
padronDF.select("desc_barrio").distinct().show(132)

# COMMAND ----------

# Muestra el número de combinaciones distintas de barrios y distritos
padronDF.select("desc_distrito", "desc_barrio").distinct().count()

# COMMAND ----------

# Crea una view del DF
padronDF.createOrReplaceTempView("padron")

# COMMAND ----------

# Muestra el número de barrios distintos
spark.sql("select count(distinct desc_barrio) from padron").show()
spark.sql("select distinct desc_barrio from padron").show(132)

# COMMAND ----------

# Añade una columna al DF con la longitud del distrito de cada fila
padronDF = padronDF.withColumn("longitud", length(padronDF["DESC_DISTRITO"]))

# COMMAND ----------

# Añade una nueva columna que todos sus datos son 5
padronDF = padronDF.withColumn("nueva_columna", lit(5))

# COMMAND ----------

# Elimina la columna anterior
padronDF = padronDF.drop("nueva_columna")

# COMMAND ----------

padronDF.show()

# COMMAND ----------

# Crea un particionado a partir de los barrios y distritos
padronparticionadoDF = padronDF.repartition("DESC_DISTRITO", "DESC_BARRIO")

# COMMAND ----------

# Muestra el número de particiones del DF del particionado
padronparticionadoDF.rdd.getNumPartitions()
# A la hora de crear el DF particionado puedo especificar el número de particiones que quiero, como por ejemplo 133 que es el número de combinaciones únicas de las columnas desc_distrito y desc_barrio, si no Spark decidirá automáticamente el número de particiones (3 en este caso)
# padronparticionadoDF = padronDF.repartition(133, "DESC_DISTRITO", "DESC_BARRIO")

# COMMAND ----------

# Guarda el DF en cache
padronparticionadoDF.cache()
# Consultar en la pestaña SparkUI del clúster y mirar en Storage

# COMMAND ----------

# Crea una view del particionado
padronparticionadoDF.createOrReplaceTempView("padronParticionado")

# COMMAND ----------

# Muestra el número total de "espanoleshombres", "espanolesmujeres", extranjeroshombres" y "extranjerosmujeres" para cada barrio de cada distrito. Las columnas distrito y barrio deben ser las primeras en aparecer en el show. Los resultados deben estar ordenados en orden de más a menos según la columna "extranjerosmujeres" y desempatarán por la columna "extranjeroshombres".
spark.sql("""
SELECT 
    desc_distrito AS distrito, 
    desc_barrio AS barrio, 
    SUM(espanoleshombres) AS total_espanoleshombres,
    SUM(espanolesmujeres) AS total_espanolesmujeres,
    SUM(extranjeroshombres) AS total_extranjeroshombres,
    SUM(extranjerosmujeres) AS total_extranjerosmujeres
FROM padronParticionado
GROUP BY distrito, barrio
ORDER BY total_extranjerosmujeres DESC, total_extranjeroshombres DESC
""").show(200)

# COMMAND ----------

# Deja de guardar el DF en cache
padronparticionadoDF.unpersist()

# COMMAND ----------

# Crea un nuevo DataFrame a partir del original que muestre únicamente una columna con DESC_BARRIO, otra con DESC_DISTRITO y otra con el número total de "espanoleshombres" residentes en cada distrito de cada barrio. Por último, lo une con un join con el DataFrame original a través de las columnas en común.
newpadronDF = padronDF.select("desc_distrito", "desc_barrio", "espanoleshombres")
newpadronDF = newpadronDF.groupBy("desc_distrito", "desc_barrio").agg(sum("espanoleshombres").alias("total_espanoleshombres"))
newpadronDF = newpadronDF.select("desc_distrito", "desc_barrio", "total_espanoleshombres")
joinpadronDF = padronDF.join(newpadronDF, on=["desc_distrito", "desc_barrio"], how="inner")

# COMMAND ----------

# Import para poder hacer operaciones de ventana
from pyspark.sql.window import *

# COMMAND ----------

# Misma consulta anterior pero con funcion de ventana
windowf = Window.partitionBy("desc_distrito", "desc_barrio")
overf = padronDF.withColumn(
    "total_espanoleshombres",
    sum("espanoleshombres").over(windowf)
)
windowpadronDF = overf.select("DESC_BARRIO", "DESC_DISTRITO", "total_espanoleshombres").distinct()

windowpadronDF.show(300)

# COMMAND ----------

# Mediante una función Pivot muestra una tabla (que va a ser una tabla de contingencia) que contenga los valores totales ()la suma de valores) de espanolesmujeres para cada distrito y en cada rango de edad (COD_EDAD_INT). Los distritos incluidos deben ser únicamente CENTRO, BARAJAS y RETIRO y deben figurar como columnas .
distritos = ["BARAJAS", "CENTRO", "RETIRO"]
padronfiltrado = padronDF.filter(padronDF["desc_distrito"].isin(distritos))
pivotpadron = padronfiltrado.groupBy("cod_edad_int").pivot("desc_distrito",distritos).sum("espanolesmujeres")
pivotpadron = pivotpadron.orderBy("cod_edad_int")
pivotpadron.show(500)

# COMMAND ----------

# Utilizando este nuevo DF, crea 3 columnas nuevas que hagan referencia a qué porcentaje de la suma de "espanolesmujeres" en los tres distritos para cada rango de edad representa cada uno de los tres distritos. Debe estar redondeada a 2 decimales.
porcentajes = pivotpadron.withColumn("porcentaje_BARAJAS", 
    ((col("BARAJAS") / (col("BARAJAS") + col("CENTRO") + col("RETIRO"))) * 100).cast("decimal(10,2)")) \
    .withColumn("porcentaje_CENTRO", 
    ((col("CENTRO") / (col("BARAJAS") + col("CENTRO") + col("RETIRO"))) * 100).cast("decimal(10,2)")) \
    .withColumn("porcentaje_RETIRO", 
    ((col("RETIRO") / (col("BARAJAS") + col("CENTRO") + col("RETIRO"))) * 100).cast("decimal(10,2)"))

porcentajes.show(500)


# COMMAND ----------


