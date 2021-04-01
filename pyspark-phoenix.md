# Pyspark con Apache Phoenix

### Consola de Pyspark

```sh
export PYTHONIOENCODING=utf8
/usr/hdp/current/spark2-client/bin/pyspark \
--name jobname \
--queue default \
--master yarn \
--conf "spark.driver.extraClassPath=/usr/hdp/current/phoenix-client/phoenix-spark2.jar:/usr/hdp/current/phoenix-client/phoenix-client.jar:/etc/hbase/conf" \
--conf "spark.executor.extraClassPath=/usr/hdp/current/phoenix-client/phoenix-spark2.jar:/usr/hdp/current/phoenix-client/phoenix-client.jar:/etc/hbase/conf" \
--num-executors 20 \
--executor-memory 8g \
--executor-cores 1 \
--driver-memory 3g

```


### Lectura

```python
tableName="ESQUEMA.TABLA"
zookeeperQuorum = "server1,server2,server3:2181/hbase-unsecure"
df = spark.read\
    .format("org.apache.phoenix.spark")\
    .option("table", tableName)\
    .option("zkUrl", zookeeperQuorum)\
    .load()

df.createOrReplaceTempView("data")
vSQL = "SELECT * FROM data LIMIT 3"
dfSQL = spark.sql(vSQL)
dfSQL.show()
```

### Escritura

```python
tableName="ESQUEMA.TABLA"
zookeeperQuorum = "server1,server2,server3:2181/hbase-unsecure"

dfHive.write.format("org.apache.phoenix.spark")\
    .mode("overwrite")\
    .option("table",phoenixTableName)\
    .option("zkUrl",zookeeperQuorum)\
    .save()


```
