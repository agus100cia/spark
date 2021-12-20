## Instalar Spark en MAC

- 1.- Instala Java 8, descarga el instalador de jdk8 desde la pagina de oracle

https://www.oracle.com/java/technologies/javase/javase8-archive-downloads.html

- 2.- Instala Python 3.9. Descarga e instala python 3.10

https://www.python.org/downloads/release/python-3101/

- 3.- Descarga Spark 3, descomprime en una carpeta

https://spark.apache.org/downloads.html

- 4.- Configura las variables de ambiente

```sh
## MAC 10.14
nano ~/.bashrc

## MAC 10.15
nano ~/.zshrc

# Spark
export SPARK_HOME="/opt/spark"
export PATH=$SPARK_HOME/bin:$PATH
export SPARK_LOCAL_IP='127.0.0.1'

## Actualiza variables
source ~/.zshrc

``` 

- 5.- Instala Jupyter

```sh
brew install pipenv
pip install --user pipenv
pipenv install jupyter

nano ~/.zshrc

# Pyspark 
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
export PYSPARK_PYTHON=python3


```

Para iniciar Jupyter

```sh
cd /folder/project
pipenv shell
pyspark
(Se abre el navegador con Jupyter)

``` 
