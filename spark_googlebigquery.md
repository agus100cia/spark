# Spark + Google Big Query

Para poder conectar Spark On Premise con Google Big Query, necesitamos una libreria de Java llamada spark-bigquery. Esta libreria se encuentra dentro de los paquetes de gsutil.

gsutil es una aplicacion Python que te permite acceder a Google Cloud Storage desde la linea de comandos. El comando "gsutil cp" permite copiar archivos entre tu sistema de archivos local al storage de google. Antes de poder usar "gsutil" es necesario instalar Cloud SDK.

## Instalar Cloud SDK en Centos

Ref: https://cloud.google.com/sdk/docs/install

- 1.- Creamos el repositorio

```sh
sudo tee -a /etc/yum.repos.d/google-cloud-sdk.repo << EOM
[google-cloud-cli]
name=Google Cloud CLI
baseurl=https://packages.cloud.google.com/yum/repos/cloud-sdk-el8-x86_64
enabled=1
gpgcheck=1
repo_gpgcheck=0
gpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg
       https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg
EOM
``` 

- 2.- Instalamos los paquetes

```sh
sudo yum install google-cloud-cli

``` 

- 3.- Inicia la consola. Si la maquina donde estas accediendo no tiene un navegador web, te generará un comando el cual deberás ejecutarlo en otro computador que si tenga un navegador web, y el resultado de consola lo copias y pegas en el servidor original.

```sh
gcloud init

You must log in to continue. Would you like to log in (Y/n)?  Y

``` 

- 4.- Ahora puede compiar archivos a GCP. Descargamos el jar de GCP para Spark

```sh
gsutil cp gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar /home/admin/jars

``` 

## Generar las credenciales en GCP

- 1.- accede a la consola de GCP
- 2.- Selecciona el proyecto
- 3.- En el menu de hambuguesa => API's & Services => Credentials => Create credentials => Service Account
- 4.- Nombre de la cuenta de servicio: bigquery-dwh  (Create)
- 5.- Rol: Proyecto => Propietario
- 6.- Another Rol: BigQuery => Administrator (Continue) 


Una vez creada la cuenta de servicio, seleccionala => Keys => Add key => Create Key => JSON (Create)

Se descarga un archivo json y Coloca el archivo de Keys en una carpeta del SO /home/admin/gcp/gcp-bigdata.json

## Conectar Pyspark con Google Big Query

Como requisitos debes:

- Tener instalado el SDK de GCP
- Haber iniciado sesion en GCP desde la consola
- Tener el archivo JSON de credenciales
- Tener habilitados los permisos de API & Services

Script Python

Asegurate de usar la version adecuada para Scala 2.11 o 2.12

- Scala 2.11. : com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.17.1
- Scala 2.12. : com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.17.1

```python

from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .master("local")\
    .appName("spark-bigquery")\
    .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.17.1')\
    .config("credentialsFile","/home/admin/gcp/gcp-bigdata.json")\
    .getOrCreate()

df = spark\
    .read\
    .format("bigquery")\
    .option("project","<codigo-pryecto-bigquery>")\
    .option("table","<esquema.tabla-managed>")\
    .load()

df.printSchema()

``` 



