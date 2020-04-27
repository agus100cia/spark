from pyspark.sql import SparkSession
# using SQLContext to read parquet file
from pyspark.sql import SQLContext
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import from_unixtime
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
from cryptography.fernet import Fernet
import base64
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import sys
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import regexp_replace, col

vConf = SparkConf(). \
        setAppName("df01_dpa"). \
        set("spark.speculation", "true").\
        set("spark.yarn.executor.memoryOverhead","2048").\
        set("spark.driver.memoryOverhead","2048").\
        setMaster("yarn")
sc = SparkContext(conf=vConf)
hc = HiveContext(sc)

vSQlDs00 = "SELECT idrow, nombres, primerapellido, primernombre  FROM externo.ds01"
ds00 = hc.sql(vSQlDs00)
ds00.printSchema()
ds00.show(3)

ds00.write.format('jdbc').options(
      url='jdbc:mysql://nodo1/persona',
      driver='com.mysql.jdbc.Driver',
      dbtable='ds00',
      user='cloudera',
      password='cliksoft').mode('overwrite').save()

## spark-submit --jars /path/mysql-jdbc.jar --master yarn script.py