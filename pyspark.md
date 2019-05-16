### Ejemplos con Pyspark

# Leer un archivo como RDD y guardarlo como parquet

´´´  
%pyspark
rdd = sc.textFile("hdfs://192.168.0.225:8020/user/admin/data/iess/IESS_Tota_utf8l.txt").zipWithIndex().filter(lambda x: x[1]>=0)
df = rdd.toDF(["cadena","id"])
df.coalesce(1).write.save("hdfs://192.168.0.225:8020/user/admin/data/iess_number");  ##escribe como parquet
´´´ 

