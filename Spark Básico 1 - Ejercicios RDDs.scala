// Define el dataframe a partir de la lectura del archivo
val df = spark.read.text("/FileStore/tables/singapur.csv").rdd

// Convierte el dataframe en un RDD si no usara el .rdd antes
// val singapurRDD = df.rdd

// COMMAND ----------

// Define variable con la ruta del directorio que contiene los archivos de texto
val directorio = "/FileStore/tables"

// Lee todos los archivos de texto del directorio en un RDD de pares (nombre_archivo, contenido_archivo)
val directorioRDD = sc.wholeTextFiles(directorio)

// Transforma el RDD con map en uno nuevo que contiene solo el contenido de cada elemento. También podría hacerse así: val contenidoRDD = directorioRDD.map(_._2)
val contenidoRDD = directorioRDD.map { case (_, contenido) => contenido }


// COMMAND ----------

// Define variable con la ruta del directorio que contiene los archivos de texto con * al final para indicar que se deben incluir todos los archivos del directorio
val directorio = "/FileStore/tables/*"

// Lee el archivo línea por línea y convierte cada una en un elemento del RDD
val directorioRDD = sc.textFile(directorio)

// COMMAND ----------

// Se le aplica una condición al RDD para que solo devuelva las lineas que contengan "filtro"
val directorioFiltrado = directorioRDD.filer(row => row.contains("filtro"))

// COMMAND ----------

// Define variable con la ruta del directorio que contiene los archivos de texto que su nombre empieza por singapur
val directorio = "/FileStore/tables/singapur*"

// Lee el archivo línea por línea y convierte cada una en un elemento del RDD
val directorioRDD = sc.textFile(directorio)

// Lee todos los archivos que son un csv
// val directorio = /FileStore/tables"
// val directorioRRDD = sc.textFile(s"$directorio/*.csv")

// COMMAND ----------

// Lista de rutas de directorios que contienen archivos de texto
val directorios = Seq(
  "/FileStore/tables/1",
  "/FileStore/tables/2",
  ...
)

// Combina los archivos de texto de los directorios en un solo RDD
val directoriosRDD = sc.textFile(directorios.mkString(","))
