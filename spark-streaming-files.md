## Spark streaming read files from folder

En este ejemplo ejecutaremos un proceso de spark streaming para leer los archivos de una carpeta


Codigo:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

spark = SparkSession.builder\
    .master("local[1]")\
    .appName("Files")\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([StructField('data', StringType(), True)])
df0 = spark.readStream\
    .schema(schema)\
    .csv("file:///home/naadbd01/test/pyspark/streaming/files/*")

df0.printSchema()

df1 = df0.select("data").groupBy("data").count()
df1.printSchema()

df1.\
    writeStream.\
    format("console").\
    outputMode("complete").\
    start().\
    awaitTermination()

```


Prueba:

```sh
echo 'Frase01' > /home/naadbd01/test/pyspark/streaming/files/foo01.txt
echo 'Frase02' > /home/naadbd01/test/pyspark/streaming/files/foo02.txt
echo 'Frase03' > /home/naadbd01/test/pyspark/streaming/files/foo03.txt

```

