## Instalar Jupyter notebook

```sh
## instalar Jupyter
pip install notebook
pip install findspark

## Crea la carpeta de los notebooks
sudo mkdir /jupyter
sudo chown -R admin:admin /jupyter

##Levantar el servicio
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark
export JAVA_HOME=/usr/java/jdk1.8.0_181-cloudera/


nohup jupyter notebook --ip=0.0.0.0 --port=8880 &

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
