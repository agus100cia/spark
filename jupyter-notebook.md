## Instalar Jupyter notebook

```sh
sudo mkdir -p /var/log/jupyter
sudo chown -R admin:admin /var/log/jupyter
```
## Instala pip 20.3

```sh
wget https://bootstrap.pypa.io/pip/2.7/get-pip.py 
python get-pip.py
``` 

## instalar Jupyter

```sh
sudo yum install python-devel
sudo yum install libevent-devel
sudo yum -y install gcc
sudo yum install python34-devel
sudo yum install python36-devel
sudo yum install lsof


sudo python3 -m pip install --upgrade setuptools
sudo python3 -m pip install --upgrade setuptools_scm
sudo python3 -m pip install --upgrade cffi
sudo python3 -m pip install JayDeBeApi
sudo python3 -m pip install impyla
sudo python3 -m pip install matplotlib==3.2.0
sudo python3 -m pip install notebook
o
sudo pip install --user --upgrade jupyter

sudo python3 -m pip install pyspark
sudo python3 -m pip install findspark
``` 

## Crea la carpeta de los notebooks

```sh 
sudo mkdir /jupyter
sudo chown -R admin:admin /jupyter
```

## Levantar el servicio

```sh
export PYSPARK_DRIVER_PYTHON=/jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark
export JAVA_HOME=/usr/java/jdk1.8.0_181-cloudera/

jupyter notebook --ip=0.0.0.0 --port=8880 --notebook-dir=/jupyter > /var/log/jupyter/jupyter.log  2>&1 &

``` 

Codigo Spark

```python
import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.\
    builder.\
    appName("jupyter").\
    master("yarn").\
    enableHiveSupport(). \
    config('spark.sql.debug.maxToStringFields', 100). \
    config('spark.debug.maxToStringFields', 100). \
    config("spark.network.timeout","60s"). \
    getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

``` 
