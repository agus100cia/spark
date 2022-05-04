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
sudo pip install notebook
sudo pip install findspark
``` 

## Crea la carpeta de los notebooks

```sh 
sudo mkdir /jupyter
sudo chown -R admin:admin /jupyter
```

## Levantar el servicio

```sh
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark
export JAVA_HOME=/usr/java/jdk1.8.0_181-cloudera/

jupyter notebook --ip=0.0.0.0 --port=8880 > /var/log/jupyter/jupyter.log  2>&1 &

``` 

Codigo Spark

```python
import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.\
    builder.\
    appName("jupyter").\
    enableHiveSupport(). \
    config('spark.sql.debug.maxToStringFields', 100). \
    config('spark.debug.maxToStringFields', 100). \
    config("spark.network.timeout","60s"). \
    getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

``` 