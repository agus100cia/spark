## Spark como Database

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

spark = SparkSession \
    .builder \
    .appName('spark') \
    .enableHiveSupport()\
    .config('spark.sql.hive.thriftServer.singleSession', True)\
    .config('hive.server2.thrift.port', '10001') \
    .getOrCreate()
    
sc=spark.sparkContext
sc.setLogLevel('INFO')

#Order matters! 
java_import(sc._gateway.jvm, "")
sc._gateway.jvm.org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.startWithContext(spark._jwrapped)
    
spark.sql("use default")
spark.sql("show tables").show()
    
df0 = spark.read.csv("file:///home/adminspark/CSV/Datos_de_Vuelos_qvd.CSV",header=True)
df0.write.mode("overwrite").saveAsTable("default.datos_vuelos")

df1 = spark.read.csv("file:///home/adminspark/CSV/aereolineas.csv",header=True)
df1.write.mode("overwrite").saveAsTable("default.aereolineas")

df2 = spark.read.csv("file:///home/adminspark/CSV/aeronaves.csv",header=True)
df2.write.mode("overwrite").saveAsTable("default.aeronaves")

``` 
