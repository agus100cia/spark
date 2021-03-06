### Ejemplos con Pyspark

# 1.- Leer un archivo como RDD y guardarlo como parquet

````python 
%pyspark

## Lee el archivo plano y les asigna un RowNumber
## Luego es filtrado por las lineas mayores a 0
rdd = sc.textFile("hdfs://ip-servidor:8020/user/admin/data/folder/fuente.txt").zipWithIndex().filter(lambda x: x[1]>=0)
## Se convierte el RDD a DF asignando un nombre a cada columna
df = rdd.toDF(["cadena","id"])
## Guarda el DF como parquet
df.coalesce(1).write.save("hdfs://ip-servidor:8020/user/admin/data/data_number");  ##escribe como parquet

```` 


# 2.- Quitar los saltos de linea de un archivo

```python

%pyspark

from pyspark.conf import SparkConf
from pyspark import SparkContext


import os
import re

##Lee todo el archivo como una sola cadena de texto
##Reemplaza los saltos de linea por |

rdd = sc.wholeTextFiles("hdfs://ip-server:8020/user/admin/data/fuente/limpio/part-00000")\
    .map(lambda x: re.sub(r'(?!(([^"]*"){2})*[^"]*$),', ' ', x[1].replace("\r\n", "|"))  )\
    
## Se reeplaza 12,00| por 12,00;
reemplazo = rdd.map(lambda x: x.replace("12,00|", "12,00;"))  

## Se hacen columnas tomando al ; como separador
cols = reemplazo.map( lambda x : x.split(";"))
  
##Se hacen grupos de 20 columnas para formar una fila 
columnas=20 
matriz = cols.flatMap(lambda x: [x[k:k+columnas] for k in range(0, len(x), columnas)])

##Si quiere verse todo unido
todo = matriz.map(lambda x: ";".join(x))    
    
 

todo.saveAsTextFile('hdfs://ip-server:8020/user/admin/data/data_partes/part1-00000')

```` 

# 3.- Lee un archivo como RDD y lo guarda partido en 5280 partes

````python 

rddData = sc.textFile("hdfs://ip-server:8020/user/admin/data/folder",5280);
rddData.saveAsTextFile("hdfs://ip-server:8020/user/admin/data/process/folder1");

```` 

# 4.- Aplicar una expresion regular como filtro en un RDD

```` python 

import re

for x in range(34,35):
    cadena = ""
    if len(str(x)) == 1: cadena = "000" + str(x) 
    elif len(str(x)) == 2: cadena = "00" + str(x)
    elif len(str(x)) == 3: cadena = "0" + str(x)
    elif len(str(x)) == 4: cadena =  str(x)
    rutain = "hdfs://ip-server:8020/user/admin/data/process/data3/part-0" + cadena
    rutaout = "hdfs://ip-server:8020/user/admin/data/process/data3/part-0" + cadena
    rdd = sc.textFile(rutain)
    
    regex_num = re.compile("^\d{10};")
    rdd1 = rdd.filter(lambda x : not regex_num.match(x))
    rdd2 = rdd1.take(rdd1.count())
    
    for y in rdd2:
       print(y + "\n")
       
 ```` 
 
 # 5.- Poner en producción un programa en PySpark
 
 5.1.- Crear un script en el sistema opertivo
 
 ````shell
 #nano program.py
 
 `````
 
 ```` python 
 
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

import os
import re


spark = SparkSession.builder.appName("PySparklProgram").getOrCreate()
sc = spark.sparkContext

for x in range(2861,5280):

    cadena = ""
    if len(str(x)) == 1: cadena = "000" + str(x)
    elif len(str(x)) == 2: cadena = "00" + str(x)
    elif len(str(x)) == 3: cadena = "0" + str(x)
    elif len(str(x)) == 4: cadena =  str(x)

    rutain = "hdfs://ip-server:8020/user/admin/data/process/data/part-0" + cadena
    rutaout = "hdfs://ip-server:8020/user/admin/data/process/data2/part-0" + cadena



    rdd =  sc.wholeTextFiles(rutain)\
    .map(lambda x: re.sub(r'(?!(([^"]*"){2})*[^"]*$),', ' ', x[1].replace("\n", "|"))  )\

    ## Se reeplaza 12,00| por 12,00;
    rddSinBL = rdd

    rddSinBL.saveAsTextFile(rutaout)
    
 `````
 
 5.2.- Ejecutar vía consola con Spark Submit
 
 ```` shell
 
 spark-submit \
--master yarn-client \
--num-executors 4 \
--executor-memory 1G \
--executor-cores 2 \
--driver-memory 1G \
program.py

```` 

5.3.- Para ejecutarlo en modo background y escribir los logs en un archivo

```` shell

spark-submit \
--master yarn-client \
--num-executors 4 \
--executor-memory 1G \
--executor-cores 2 \
--driver-memory 1G \
program.py 2> errorOutput.log > output.log &

````` 

## 6.- Leer archivos desde Azure

````python

from pyspark.sql.types import *

ruta = "wasbs:///example/data/folder/original/archivo.txt"

rddcedulas = sc.textFile(ruta)
rddcedulas.count()

```` 

## 7.- Ver todas las variables de configuración de Spark Context

```  
for item in sorted(sc._conf.getAll()): print(item)

```

## 8.- Spark SQL

```python 
ruta = "hdfs://ip-server:8020/user/admin/data/"
rdd = sc.textFile(ruta).zipWithIndex().filter(lambda x: x[1]>=1)
df = rdd.toDF(["texto","id"])
df.createOrReplaceTempView("tabla")
spark.sql("SELECT * FROM tabla").show()
```` 

## 9.- RDD save into HIVE

```python
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
sc.setSystemProperty("hive.metastore.uris", "thrift://namenode:9083")
sparkSession = (SparkSession
                .builder
                .appName('example-pyspark-read-and-write-from-hive')
                .enableHiveSupport()
                .getOrCreate())
rutaIess = "hdfs://ip-server:8020/user/admin/data/iess"
rdd = sc.textFile(rutaIess).zipWithIndex().filter(lambda x: x[1]>=1)
df = rdd.toDF(["texto","id"])
df.write.saveAsTable('esquema.tabla_texto2')

```` 

## 10.- Mostrar logs solo en caso de ERROR 

```py
spark = SparkSession.builder.appName('app').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')


``` 
