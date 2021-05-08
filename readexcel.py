##Primero se debe instalar en todos los nodos del cl¡uster las lib
## sudo yum install python-pip
## sudo pip install xlrd

## Ejecucion
# spark-submit \
# --jars spark-excel_2.11-0.12.2.jar,commons-collections4-4.3.jar,xmlbeans-3.1.0.jar,ooxml-schemas-1.4.jar \
# --executor-memory 4g \
# --driver-memory 6g \
# readexcel.py


# spark-submit \
# --packages com.github.zuinnote:spark-hadoopoffice-ds_2.11:1.0.4,com.crealytics:spark-excel_2.11:0.12.5 \
# --jars spark-excel_2.11-0.12.2.jar,commons-collections4-4.3.jar,xmlbeans-3.1.0.jar,ooxml-schemas-1.4.jar \
# --master local \
# --executor-memory 8g \
# --driver-memory 12g \
# readexcel_apetito_riesgo_scores.py

from pyspark.sql import functions as F
from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, DoubleType, DateType, StringType, FloatType, TimestampType
import datetime
import xlrd
from datetime import datetime
from pyspark.sql.functions import udf


spark = SparkSession.builder.appName("readexcel").enableHiveSupport().getOrCreate()
df = spark.read.format("com.crealytics.spark.excel") \
.option("useHeader", "true") \
.option("inferSchema", "true") \
.option("sheetName", 'Sheet1') \
.load("file:///home/admin/etl/externo/apetito_riesgo_con_scores.xlsx")
df.printSchema()
df.repartition(1).write.mode("overwrite").saveAsTable("externo.apetito_riesgo_con_scores")
spark.stop()




### Si el excel supera los 300 Mb se debe agregar la opcion maxRowsInMemory
## pyspark --packages com.github.zuinnote:spark-hadoopoffice-ds_2.11:1.0.4

spark = SparkSession.builder.appName("readexcel").enableHiveSupport().getOrCreate()
df = spark.read.format("com.crealytics.spark.excel") \
.option("maxRowsInMemory",100000000) \
.option("useHeader", "true") \
.option("inferSchema", "true") \
.option("sheetName", 'Sheet1') \
.load("file:///home/admin/etl/externo/apetito_riesgo_con_scores.xlsx")
df.printSchema()
df.repartition(1).write.mode("overwrite").saveAsTable("externo.apetito_riesgo_con_scores")
spark.stop()
