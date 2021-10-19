# spark
Notas de desarrollo en spark

Dado que el desarrollo BigData para américa latina es bastante limitado, es difícil encontrar fuentes de información o pedazos de código que nos ayuden a desarrollar programas de análisis de datos con spark en español.

Por eso he decidido crear este repositorio para ir agregando mis experiencias con spark en sus distintas versiones.

Espero les sirva tanto como a mí

## CASO DE USO SPARK

Felipao gracias por tu ayuda. 

Te doy una pequeña introduccion a lo que es SPARK.

Spark es un framework de procesamiento distribuido, ahorita vas a ejecutar spark sobre un cluster de 5 servidores, Spark controla internamente el paralelizmo de los procesos y los try/catch en caso de que un servidor deje de funcionar, básicamente tu debes centrarte en el código.

SPARK puedes programar en JAVA, SCALA, R, PYTHON. En este IDE (Zeppelin), he configurado para programar en Scala, Python o R.
Lo que debes hacer es crear un párrafo y antes de nada poner el tag del lenguaje en el que vas a programar. Ejemplo

Scala = %spark
Python = %pyspark


En spark se maneja el concepto de RDD, esto es como decir una tabla en una base de datos, basicamente es una matriz de datos sobre el cual puedes ejecutar procesos en forma de transformaciones y acciones. 
Las transformaciones es decirle a spark lo que tiene que hacer
Las acciones es cuando spark ya lo hace.

Te voy a dar 2 RDD

rdd test  El objetivo es consultar el nombre padre en el RDD rdd catalogo y obtener la cedula en los casos que coincida
```
-----------------------------------------------------------------------------------------------------
cedula      |nombre                               |cedulapadre  |nombrepadre                     
-----------------------------------------------------------------------------------------------------
1803624327	|GUAYGUA REYES MARIA CRISTINA	 	  |              |GUAYGUA TONATO LUIS HERNAN
1803624525	|BARROSO JINEZ MONICA DE LOS ANGELES  |	 	         |BARROSO AMAN LUIS ERNESTO
1803599750	|DIAZ CONTERON DANIELA ALEXANDRA      |	 	         |ALBERTO DIAZ
1803599826	|CHACHA GUANGASI MARIA PIEDAD	 	  |              |**********************
1803599917	|VELASTEGUI LUZURIAGA MONICA ISABEL	  | 	         |VELASTEGUI ANDALUZ RODRIGO ABEL
-----------------------------------------------------------------------------------------------------
```

rdd_catalogo = Tiene un listado de cedulas y nombres
```
-----------------------------------------------------------------------------------------------------
cedula      |nombre                    
-----------------------------------------------------------------------------------------------------
0650531700	|CHAFLA GUAMAN JONATAN ALEJANDRO
0650531718	|HIDALGO TOLEDO ANAELA SALOME
1801188747	|GUAYGUA TONATO LUIS HERNAN
1600078586	|BARROSO AMAN LUIS ERNESTO
1801032010	|VELASTEGUI ANDALUZ RODRIGO ABEL
-----------------------------------------------------------------------------------------------------
```

Para esto considero que puedes usar 2 funciones map o faltMap y filter.

MAP o FLATMAP: Te permite aplicar una funcion por cada registro del RDD
FILTER: Es como hacer un where en una tabla SQL

Te dejo un par de ejemplos en scala:

MAP:

````scala

val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split(" "))
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)
counts.saveAsTextFile("hdfs://...");

Te dejo el link oficial:

https://spark.apache.org/examples.html

````


## Pandas

```sh
sudo yum -y install gcc
sudo yum install python-devel
sudo yum install libevent-devel

sudo yum install epel-release
sudo yum install python-pandas

sudo pip install -U setuptools
sudo pip install numpy==1.7.0
sudo pip2 install pandas==0.20.3
sudo pip install xlrd==1.2.0
``` 

## Usar la Ñ en codigo

En la primera linea del script de python se debe colocar

```python

# -*- coding: utf-8 -*-

``` 

### Upgrade Python 3.9 en Centos

https://phoenixnap.com/kb/how-to-install-python-3-centos-7

## Comparar 2 dataframes

Se intenta comparar 2 dataframes y mostrar las columnas que tienen variacion

- Metodo 1

```py

from pyspark.sql.functions import col, array, when, array_remove
from pyspark.sql.functions import udf,lit,struct,concat_ws,when


df1 = spark.createDataFrame([
  [1, "ABC", 5000, "US"],
  [2, "DEF", 4000, "UK"],
  [3, "GHI", 3000, "JPN"],
  [4, "JKL", 4500, "CHN"]
], ["id", "name", "sal", "Address"])

df2 = spark.createDataFrame([
  [1, "ABC", 5000, "US"],
  [2, "DEF", 4000, "CAN"],
  [3, "GHI", 3500, "JPN"],
  [4, "JKL_M", 4800, "CHN"]
], ["id", "name", "sal", "Address"])


columns = df1.columns
df3 = df1.alias("d1").join(df2.alias("d2"), col("d1.id") == col("d2.id"), "left")

for name in columns:
    df3 = df3.withColumn(name + "_temp", when(col("d1." + name) != col("d2." + name), lit(name)))


df4 = df3.withColumn("column_names", concat_ws(",", *map(lambda name: col(name + "_temp"), columns))).select("d1.*", "column_names")
df4.show()


``` 


- Metodo 2

```py

from pyspark.sql.functions import col, array, when, array_remove
from pyspark.sql.functions import udf,lit,struct


df1 = spark.createDataFrame([
  [1, "ABC", 5000, "US"],
  [2, "DEF", 4000, "UK"],
  [3, "GHI", 3000, "JPN"],
  [4, "JKL", 4500, "CHN"]
], ["id", "name", "sal", "Address"])

df2 = spark.createDataFrame([
  [1, "ABC", 5000, "US"],
  [2, "DEF", 4000, "CAN"],
  [3, "GHI", 3500, "JPN"],
  [4, "JKL_M", 4800, "CHN"]
], ["id", "name", "sal", "Address"])

df = df1
df1 = df2

df2 = df.select([col(c).alias("x_"+c) for c in df.columns])
df3 = df1.join(df2, col("id") == col("x_id"), "left")

def CheckMatch(Column,r):
    check=''
    ColList=Column.split(",")
    for cc in ColList:
        if(r[cc] != r["x_" + cc]):
            check=check + "," + cc
    return check.replace(',','',1).split(",")
    
CheckMatchUDF = udf(CheckMatch)

finalCol = df1.columns
finalCol.insert(len(finalCol), "column_names")

df4 = df3.withColumn("column_names", CheckMatchUDF(lit(','.join(df1.columns)),struct([df3[x] for x in df3.columns]))).select(finalCol)

df4.show()


``` 



