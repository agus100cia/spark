# Conexion de Spark a bases de datos

## Spark con Oracle


```python

from pyspark.sql import SparkSession
import time
from pyspark.sql import functions as F

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

vDriver='oracle.jdbc.driver.OracleDriver'
vUrl='jdbc:oracle:thin:@10.5.1.22:7594/CPP11'
vUser='rnaclaro'
vPassword='muydificil'
vTabla='roamnac.OTC_T_TRAFICO_voz_in'

df = spark.read.format('jdbc').\
    option('driver',vDriver).\
    option('url',vUrl).\
    option('dbtable',vTabla).\
    option('user',vUser).\
    option('password',vPassword).\
    load()

dfX = df.where("DATE_START like '20210504%'")

dfX.repartition(1).write.mode("overwrite").saveAsTable("db_desarrollo2021.oraclespark")

spark.stop()

```

Ejecutar:

```sh
spark-submit  --master yarn --jars ojdbc6.jar  spark-import.py

```  
