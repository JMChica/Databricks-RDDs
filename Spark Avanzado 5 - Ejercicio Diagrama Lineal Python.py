# Databricks notebook source
# MAGIC %pip install matplotlib seaborn

# COMMAND ----------

# Importa librerías para crear diagramas lineales
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import *

# COMMAND ----------

# Lee el archivo csv en un dataframe
SimpsonsDF = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/FileStore/tables/simpsons.csv")

# COMMAND ----------

SimpsonsDF.printSchema()
SimpsonsDF.show()

# COMMAND ----------

# Crea una view del dataframe
SimpsonsDF.createTempView("Simpsons")

# COMMAND ----------

# Consulta SQL que muestra las temporadas en orden ascendente con su puntuación medía en IMDB de sus capítulos agrupados
datos = sqlContext.sql("SELECT CAST(season as INT), MEAN(imdb_rating) as MediaRatingIMDB FROM Simpsons GROUP BY CAST(season as INT) ORDER BY season ASC")
datos.show()

# COMMAND ----------

# Crea un PairRDD a partir de los datos de la consulta anterior
SimpsonsRDD = datos.rdd.map(lambda x: (x['season'], x['MediaRatingIMDB']))


# COMMAND ----------

# Muestra el contenido del PairRDD para comprobar que está correcto
resultados = SimpsonsRDD.collect()

for resultado in resultados:
    print(resultado)

# COMMAND ----------

# Para poder crear el diagrama lineal más tarde, necesitamos dividir el RDD anterior de dos variables para cada eje de coordenadas
X = SimpsonsRDD.map(lambda x: x[0]).collect()
Y = SimpsonsRDD.map(lambda x: x[1]).collect()
print(X)
print(Y)

# COMMAND ----------

# Crea el diagrama lineal
plt.figure(figsize=(10, 6)) # Crea una nueva figura y se le indica el tamaño en pulgadas ancho x altura
sns.lineplot(x=X, y=Y, marker='o', color='b', label='Puntuaciones Medias') # Crea el gráfico lineal indicando los datos para el eje x e y, marcar con punto cada dato, que la linea sea de color azul y aportando una etiqueta.
plt.xlabel('Temporadas')
plt.ylabel('Puntuaciones Medias')
plt.title('Puntuaciones Medias de IMDb por Temporada')
plt.xticks(X) # Indica que las marcas del eje x debe coincidir con los datos de X (las temporadas)
plt.grid(True) # Agrega una cuadrícula al gráfico para facilitar su lectura
plt.legend() # Crea una leyenda a partir de la etiqueta
plt.show()
