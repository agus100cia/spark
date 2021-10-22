## Spark como Database

NOTA: Los datos se guardan en la carpeta spark-warehouse , esta carpeta se crea en el folder donde se ejecuta spark-submit o pyspark o spark-shell

Se puede usar Spark como Base de datos.

- 1.- Descargar Spark
- 2.- Descomprimir Spark
- 3.- cd $SPARK_HOME
- 4.- Levantar el servicio Thrift de Spark

```sh

./sbin/start-thriftserver.sh  --hiveconf hive.server2.thrift.port=10001 --conf spark.executor.cores=4 --master spark://23.101.141.196:7077

``` 

- 5.- Iniciar Pyspark con conector a thrift

```sh
./bin/pyspark

```

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from py4j.java_gateway import java_import
import time

spark = SparkSession \
    .builder \
    .appName('spark') \
    .enableHiveSupport()\
    .config('spark.sql.hive.thriftServer.singleSession', True)\
    .config('hive.server2.thrift.port', '10000') \
    .getOrCreate()
    
spark.sql.warehouse.dir('file:/home/adminspark/spark-2.4.5-bin-hadoop2.7/bin/spark-warehouse/')


sc=spark.sparkContext
sc.setLogLevel('ERROR')

#Order matters! 
java_import(sc._gateway.jvm, "")
sc._gateway.jvm.org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.startWithContext(spark._jwrapped)
    
spark.sql("use default")
spark.sql("show tables").show()
    
df0 = spark.read.csv("file:///home/adminspark/datoscsvsimplificado/datos_vuelos.csv",header=True)
df0.write.mode("overwrite").saveAsTable("default.datos_vuelos")

df1 = spark.read.csv("file:///home/adminspark/datoscsvsimplificado/aerolineas.csv",header=True)
df1.write.mode("overwrite").saveAsTable("default.aereolineas")

df2 = spark.read.csv("file:///home/adminspark/datoscsvsimplificado/aeronaves.csv",header=True)
df2.write.mode("overwrite").saveAsTable("default.aeronaves")

df3 = spark.read.csv("file:///home/adminspark/datoscsvsimplificado/aeropuertos_origen.csv",header=True)
df3.write.mode("overwrite").saveAsTable("default.aeropuertos_origen")

df4 = spark.read.csv("file:///home/adminspark/datoscsvsimplificado/tipos_de_aeronave.CSV",header=True)
df4.write.mode("overwrite").saveAsTable("default.tipos_de_aeronave")

df5 = spark.read.csv("file:///home/adminspark/datoscsvsimplificado/tipos_de_aerolinea.csv",header=True)
df5.write.mode("overwrite").saveAsTable("default.tipos_de_aerolinea")

df6 = spark.read.csv("file:///home/adminspark/datoscsvsimplificado/aeropuertos_destino.csv",header=True)
df6.write.mode("overwrite").saveAsTable("default.aeropuertos_destino")

while True:
  time.sleep(5)
  
``` 


```sh

./spark/bin/spark-submit \
  --master spark://localhost:7077 \
  --total-executor-cores 2 \
  ./thriftserver-in-context.py
``` 
