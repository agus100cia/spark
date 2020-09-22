# Spark en windows

NOTA: Las versiones de Spark y Python influyen en la conexion con Cloudera Hadoop

## 1.- Instalar Java JDK 8.

Descarga Jdk 8 de:

https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html

Instala Jdk 8. Para verificar la instalación, abre la consola CMD y ejecuta

```sh

java -version

```

## Instala Python 2.7.16

Descarga Python desde (Asegurate de verificar la carpeta donde se va a instalar):

https://www.python.org/ftp/python/2.7.16/python-2.7.16.amd64.msi

Crea una variable de ambiente en el panel de control => Sistema => Variables de ambiente => Variables de sistema => Nueva => 

```sh 
PYTHON=C:\Python27
PATH=%PATH%;%PYTHON%

```

## Descarga Spark

Descarga Spark desde:

https://archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz

- Descomprime en el disco C:\spark-2.4.5-bin-hadoop2.7
- Crea una variable de ambiente SPARK_HOME=C:\spark-2.4.5-bin-hadoop2.7
- Descarga winutils.exe desde https://github.com/steveloughran/winutils/blob/master/hadoop-3.0.0/bin/winutils.exe
- Crea la carpeta C:\spark-2.4.5-bin-hadoop2.7\hadoop\bin
- Coloca winutils.exe en C:\spark-2.4.5-bin-hadoop2.7\hadoop\bin
- Crea una variable de ambiente llamada HADOOP_HOME=C:\spark-2.4.5-bin-hadoop2.7\hadoop

## Comprueba la instalación

Ejecuta desde el cmd:

```sh
C:\spark-2.4.5-bin-hadoop2.7\bin\spark-shell
C:\spark-2.4.5-bin-hadoop2.7\bin\pyspark

```
![img](https://github.com/agus100cia/spark/blob/master/Captura%20de%20Pantalla%202020-08-11%20a%20la(s)%2019.42.53.png)


Continuar leyendo ...

https://github.com/agus100cia/spark/blob/master/spark-install.md


