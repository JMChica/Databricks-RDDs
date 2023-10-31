# Databricks notebook source
# Lee el archivo de texto en una variable
log_lines = spark.read.text("/FileStore/tables/access_log_Aug95.txt")

# COMMAND ----------

# Muestra las 5 primeras líneas para verificar el texto
log_lines.show(5, truncate=False)

# COMMAND ----------

# Importa el paquete de functions porque incluye la función regexp_extract
from pyspark.sql.functions import *

# COMMAND ----------

# Define la expresión regular para leer el archivo de texto, lo dividirá en 9 grupos
expresion_regular = r'^(\S+) - - \[(\d{2}\/\w{3}\/\d{4}):(\d{2}:\d{2}:\d{2}) ([+\-]\d{4})\] "([^ ]*) (\/\S*)? (\w{4}\/\d\.\d)" (\d{3}) (\d+|-)$'

# COMMAND ----------

# Almacena los 5 grupos en un dataframe, asignandole un nombre a la columna que contiene cada grupo
logDF = log_lines.select(
    regexp_extract("value", expresion_regular, 1).alias("host"),
    regexp_extract("value", expresion_regular, 2).alias("date"),
    regexp_extract("value", expresion_regular, 3).alias("hour"),
    regexp_extract("value", expresion_regular, 4).alias("timezone"),
    regexp_extract("value", expresion_regular, 5).alias("request"),
    regexp_extract("value", expresion_regular, 6).alias("path"),
    regexp_extract("value", expresion_regular, 7).alias("protocol"),
    regexp_extract("value", expresion_regular, 8).alias("status_code"),
    regexp_extract("value", expresion_regular, 9).alias("size")
)

# COMMAND ----------

# Comprueba si se han leido correctamente los datos con la expresion regular
logDF.show(5, truncate=False)

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/nasa.parquet", recurse=True)
# Guarda el dataframe estructurado en formato parquet
logDF.write.parquet("/FileStore/tables/nasa.parquet")

# COMMAND ----------

nasaDF = spark.read.parquet("/FileStore/tables/nasa.parquet")

# COMMAND ----------

# Muestra los protocolos agrupados y las veces que aparecen cada uno
nasaDF.select("protocol").groupBy("protocol").count().show()

# COMMAND ----------

# Muestra los códigos de estado agrupados y ordenados para ver cuáles son los más utilizados
nasaDF.select("status_code").groupBy("status_code").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# Muestra los métodos de petición agrupados y ordenados para ver cuáles son los más utilizados
nasaDF.select("request").groupBy("request").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# Muestra los recursos de la web ordenados según la transferencia de bytes de cada uno
sizefilter = nasaDF.filter(nasaDF.size != "-")
sizefilter = sizefilter.withColumn("size", sizefilter["size"].cast("integer")) # La columna "size" es de tipo string y es necesario pasarla a integer para futuras operaciones
size_request = sizefilter.groupBy("path").agg(sum("size").alias("total_size")).orderBy("total_size", ascending=False)
size_request.show()

# COMMAND ----------

# Muestra los recursos de la web ordenados para ver cuáles son los más utilizados, es decir, tienen más registros
nasaDF.select("path").groupBy("path").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# Muestra los días que la web tuvo más tráfico
nasaDF.select("date").groupBy("date").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# Muestra los hosts más frecuentes
nasaDF.select("host").groupBy("host").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# Muestra las horas en las que se ha producido mayor tráfico en la web
nasaDF.select("hour").groupBy("hour").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# Muestra el número de errores 404 que ha habido cada día
statusfilter = nasaDF.filter(nasaDF.status_code == 404)
statusfilter.select("date", "status_code").groupBy("date","status_code").count().orderBy("count", ascending=False).show()
