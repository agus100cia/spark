# Pyspark Challenge

En este ejercicio trabajaremos con Pyspark usando un data set con datos del covid a nivel mundial.

## Instrucciones:

## Fuentes:

Puedes descargar la fuente de datos desde (Puedes descargar todo el proyecto de Github):

https://github.com/owid/covid-19-data/blob/master/public/data/owid-covid-data.csv

## Instancia el Spark Context en tu máquina de forma local

- Crea un proyecto de Pyspark llamado "spark-challenge-usuario" en usuario utiliza tu nombre de usuario de red (Active Directory)
- Configura los archivos .zip de Spark dentro de Pycharm=>Preference=>Project=>Project Structure=>Add Content Root=>(py4j-0.10.7-src.zip y pyspark.zip )
- Crea un archivo main.py en el paquete com.spark.challenge (Ejecútalo en blanco)
- Configura en Run=>Edit configurations=>Enviroment variables => SPARK_HOME="Path de spark"
- En el archivo main.py inicializa SparkContext y HiveContext

```python
from pyspark import SparkContext, HiveContext
sc = SparkContext().master("local[*]").appName("Spark-Challenge")
hc = HiveContext(sc)
```
- Lee el archivo CSV (Debes colocar la ruta donde se encuentra tu archivo):

```python
df_data = hc.read.option("header",True).csv("ruta/owid-covid-data.csv")

```
- Toma las columnas continent, total_cases y total_deaths para calcular la tasa de mortalidad por continente, para ello convierte a entero los campos correspondientes.

```python
df_data_cast = df_data.selectExpr(
    "continent as continente",
    "cast(total_cases as int) as total_casos",
    "cast(total_deaths as int) as total_muertes"
)

```

- Elimina los valores nulos del campo total_casos y total_death

```python
df_data_nonull = df_data_cast.where("total_casos is not null and total_muertes is not null")

```

- Sumariza el total de muertes y divide para el total de casos para obtener el campo tasa_mortalidad

```python
df_mortalidad = df_data_nonull.groupBy("continente").agg(
    (F.sum(F.col("total_muertes"))/F.sum(F.col("total_casos"))).alias("tasa_mortalidad")
)

```

- Resultado esperado:

```sh
+-------------+--------------------+
|   continente|     tasa_mortalidad|
+-------------+--------------------+
|       Europe| 0.08056830953028485|
|       Africa| 0.02360132430410403|
|         null|  0.0467565307259068|
|North America| 0.04787480224510096|
|South America| 0.03718416753570359|
|      Oceania|0.013278055167237303|
|         Asia|0.024501988996671956|
+-------------+--------------------+

```

- Escribe la tabla resultante en el clúster usando el usuario curso y la clave Ecuador01. La tabla debe llamarse "examen_usuario" (con tu nombre de usuario de red).

```python
df_mortalidad.repartition(1).write.saveAsTable("curso.examen_amartinez")

```

Para ejecutar en producción con Spark Submit

```sh
spark-submit --master yarn main.py 

````

NOTA: Para las personas que programen en Zeppelin o vayan a poner en producción su código, deben usar la ruta del HDFS para leer el archivo.

hdfs://user/curso/examen/owid-covid-data.csv
