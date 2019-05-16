### Ejemplos con Pyspark

# 1.- Leer un archivo como RDD y guardarlo como parquet

```` 
%pyspark

## Lee el archivo plano y les asigna un RowNumber
## Luego es filtrado por las lineas mayores a 0
rdd = sc.textFile("hdfs://192.168.0.225:8020/user/admin/data/folder/fuente.txt").zipWithIndex().filter(lambda x: x[1]>=0)
## Se convierte el RDD a DF asignando un nombre a cada columna
df = rdd.toDF(["cadena","id"])
## Guarda el DF como parquet
df.coalesce(1).write.save("hdfs://192.168.0.225:8020/user/admin/data/iess_number");  ##escribe como parquet

```` 


# 2.- Quitar los saltos de linea de un archivo

````
%pyspark

from pyspark.conf import SparkConf
from pyspark import SparkContext


import os
import re

##Lee todo el archivo como una sola cadena de texto
##Reemplaza los saltos de linea por |

rdd = sc.wholeTextFiles("hdfs://192.168.0.225:8020/user/admin/data/fuente/limpio/part-00000")\
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
    
 

todo.saveAsTextFile('hdfs://192.168.0.225:8020/user/admin/data/iess_partes/part1-00000')

```` 


