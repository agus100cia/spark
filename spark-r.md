## Spark con R

A continuación se muestran las instrucciones para instalar y jugar con Spark en un clúster CDH de Cloudera para que pueda jugar con SparkR. 
Actualmente, este no es un componente compatible. No recomendaría instalar esto en nodos que tengan instalado Spark en ellos actualmente; 
en su lugar, debe instalarlo en un nodo de puerta de enlace con HDFS y roles de puerta de enlace YARN instalados en él.

### 1.- Asegúrese de instalar R en el nodo Edge

```sh
sudo yum -y install curl
sudo yum -y install libcurl-devel
sudo yum -y install libxml2-devel
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

### 5.- Instalar parquete

```sh
# sh /opt/sparkr/spark-2.4.5-bin-hadoop2.7/bin/sparkR
>install.packages("readr")
>install.packages("curl")
>install.packages("tidyverse")
>install.packages("ROracle")
>install.packages("dbplyr")
>install.packages("ROracle")
``` 

### Instalar Oracle lib

```sh
sudo wget https://download.oracle.com/otn_software/linux/instantclient/213000/oracle-instantclient-basic-21.3.0.0.0-1.x86_64.rpm
sudo rpm -i oracle-instantclient-basic-21.3.0.0.0-1.x86_64.rpm

export ORACLE_INSTANT_CLIENT_VERSION=21
export ORACLE_YUM_URL=https://yum.oracle.com 
export ORACLE_HOME=/usr/lib/oracle/${ORACLE_INSTANT_CLIENT_VERSION}/client64
export ORACLE_YUM_REPO=public-yum-ol7.repo 
export ORACLE_YUM_GPG_KEY=RPM-GPG-KEY-oracle-ol7 

rpm --import ${ORACLE_YUM_URL}/${ORACLE_YUM_GPG_KEY};
curl -o /etc/yum.repos.d/${ORACLE_YUM_REPO} ${ORACLE_YUM_URL}/${ORACLE_YUM_REPO};
sed -i 's/enabled=1/enabled=0/g' /etc/yum.repos.d/${ORACLE_YUM_REPO}; 
yum-config-manager --enable ol7_oracle_instantclient;

```

### Configurar Zeppelin con Livy

En cloudera manager => Livy => Configuracion => Buscar: livy-env.sh  Livy REST Server Advanced Configuration Snippet (Safety Valve) for livy-conf/livy-env.sh

Ref: https://enterprise-docs.anaconda.com/en/latest/admin/advanced/config-livy-server.html

```sh
export JAVA_HOME=/usr/java/jdk1.8.0_181-cloudera
export SPARK_HOME=/opt/sparkr/spark-2.4.5-bin-hadoop2.7
export SPARK_CONF_DIR=$SPARK_HOME/conf
export HADOOP_HOME=/etc/hadoop/
export HADOOP_CONF_DIR=/etc/hadoop/conf

``` 

### R-Studio

Ref: https://techcommunity.microsoft.com/t5/sql-server/using-rstudio-server-with-microsoft-r-server-parcel-for-cloudera/ba-p/385449

RStudio Server Open Source License :

```sh
wget https://download2.rstudio.org/rstudio-server-rhel-1.0.143-x86_64.rpm
sudo yum install --nogpgcheck rstudio-server-rhel-1.0.143-x86_64.rpm
sudo rstudio-server verify-installation
sudo rstudio-server version

http://<nodename>:8787.

``` 

Se accede con el usuario del sistema operativo
