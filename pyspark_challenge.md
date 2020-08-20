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
- En el archivo main.py inicializa SparkContext

```python
from pyspark import SparkContext
sc = SparkContext().master("local[*]").appName("Spark-Challenge")

```

