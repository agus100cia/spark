# Spark en windows

## 1.- Instalar Java JDK 8.

Descarga Jdk 8 de:

https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html

Instala Jdk 8. Para verificar la instalación, abre la consola CMD y ejecuta

```sh

java -version

```

## Instala Python 3.8+

Descarga Python desde (Asegurate de verificar la carpeta donde se va a instalar):

https://www.python.org/ftp/python/3.8.5/python-3.8.5-amd64.exe

Crea una variable de ambiente en el panel de control => Sistema => Variables de ambiente => Variables de sistema => Nueva => 

```sh 
PYTHON=C:\Users\Administrador\AppData\Local\Programs\Python\Python38
PATH=%PATH%;%PYTHON%

```

## Descarga Spark

Descarga Spark desde:

https://downloads.apache.org/spark/spark-3.0.0/spark-3.0.0-bin-hadoop2.7.tgz

- Descomprime en el disco C:\spark-3.0.0-bin-hadoop2.7
- Crea una variable de ambiente SPARK_HOME=C:\spark-3.0.0-bin-hadoop2.7
- Descarga winutils.exe desde https://github.com/steveloughran/winutils/blob/master/hadoop-3.0.0/bin/winutils.exe
- Crea la carpeta C:\spark-3.0.0-bin-hadoop2.7\hadoop\bin
- Crea una variable de ambiente llamada HADOOP_HOME=C:\spark-3.0.0-bin-hadoop2.7\hadoop\bin

## Comprueba la instalación

Ejecuta desde el cmd:

```sh
C:\spark-3.0.0-bin-hadoop2.7\bin\spark-shell
C:\spark-3.0.0-bin-hadoop2.7\bin\pyspark

```





