## Spark con R

A continuación se muestran las instrucciones para instalar y jugar con Spark en un clúster CDH de Cloudera para que pueda jugar con SparkR. 
Actualmente, este no es un componente compatible. No recomendaría instalar esto en nodos que tengan instalado Spark en ellos actualmente; 
en su lugar, debe instalarlo en un nodo de puerta de enlace con HDFS y roles de puerta de enlace YARN instalados en él.

### 1.- Asegúrese de instalar R en el nodo Edge

```sh
sudo yum install R
``` 

### 3.- Instale Spark en el nodo Edge

- Asegúrese de tener configurado el JAVA_HOME
- Defina el SPARK_DIST_CLASSPATH


```sh
export JAVA_HOME=/usr/java/jdk1.8.0_181-cloudera/
export SPARK_DIST_CLASSPATH=$(hadoop --config /etc/hadoop/conf/ classpath)
sudo mkdir -p /opt/sparkr
cd /opt/sparkr
sudo wget https://archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz
sudo tar -xvzf spark-2.4.5-bin-hadoop2.7.tgz 

``` 

### 4.- Vincular SparkR con YARN

```sh
export SPARK_HOME="/opt/sparkr/spark-2.4.5-bin-hadoop2.7"
export HADOOP_CONF_DIR="/etc/hadoop/conf"
sh /opt/sparkr/spark-2.4.5-bin-hadoop2.7/bin/sparkR --master yarn

```

### 4.- Iniciar SparkR en local

```sh
sh /opt/sparkr/spark-2.4.5-bin-hadoop2.7/bin/sparkR

``` 
