# Instalar Spark 3.3.0

## Instalar Python 3.7.11

```sh
sudo yum -y install gcc openssl-devel bzip2-devel libffi-devel zlib-devel xz-devel 
cd /opt
sudo wget https://www.python.org/ftp/python/3.7.11/Python-3.7.11.tgz
sudo tar xzf Python-3.7.11.tgz
cd Python-3.7.11
./configure --enable-optimizations
make altinstall

```

## Instalar Spark 3.3.0

```sh
nano /opt/spark-3.3.0-bin-hadoop3/conf/spark-env.sh

JAVA_HOME=/usr/java/jdk1.8.0_181-cloudera
PYSPARK_PYTHON=/usr/local/bin/python3.7
PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.7
SPARK_HOME=/opt/spark-3.3.0-bin-hadoop3
HADOOP_CONF_DIR=/etc/hadoop/conf

/opt/spark-3.3.0-bin-hadoop3/conf/spark-defaults.conf

spark.sql.parquet.int96RebaseModeInRead CORRECTED
spark.sql.parquet.int96RebaseModeInWrite CORRECTED
spark.sql.parquet.datetimeRebaseModeInWrite CORRECTED
spark.sql.parquet.datetimeRebaseModeInRead CORRECTED

```
