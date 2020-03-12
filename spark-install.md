## Instalar  Spark en MAC

### 1.- Instalar brew

```ssh
/usr/bin/ruby -e “$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)”

````

### 2.- Instalar Xcode y Spark con Scala

````ssh
xcode-select –install
brew cask install java
brew install scala
brew install apache-spark

```` 

### 3.- Configurar HIVE con Spark

Es necesario colocar el archivo hive-site.xml dentro de la carpeta conf de Spark. Para obtener el path de spark usamos.

````ssh
brew info apache-spark
````` 

Folder: /usr/local/Cellar/apache-spark/2.4.5
Folder conf: /usr/local/Cellar/apache-spark/2.4.5/libexec/conf

### 4.- IDE PyCharm

4.1.- Instalar PyCharm

4.2.- File==>New Project ==>Create new Python File

4.3.- PyCharm ==> Preferences ==> Project:nombre ==>Project Structure ==>Add Content Root ==> Selecionar todos los archivos ZIP cd $SPARK_HOME/python/lib y aplicar

4.4.- Run ==> Edit Configurations → + → Python. Name it "Run with Spark" ==>Añadir la variable de ambiente 
SPARK_HOME=/Users/usuario/Documents/jdbc/spark-2.4.5-bin-hadoop2.7

4.5.- Colocar los archivos hive-site.xml, core-site.xml, hadoop-env.sh, hdfs-site.xml, hive-env.sh, hive-site.xml, mapred-site.xml, redaction-rules.json, ssl-client.xml, topology.map, topology.py, yarn-site.xml.

Código:

````python
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext

vConf = SparkConf().\
    setAppName("farma-saldos").\
    set("spark.speculation","true")
sc = SparkContext(conf=vConf)
sc.setLogLevel("ERROR")

hc = HiveContext(sc)
hc.sql("show databases").show()

```` 
