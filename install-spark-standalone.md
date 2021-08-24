## Install Spark Standalone


```sh
	
yum -y install epel-release
wget http://downloads.lightbend.com/scala/2.11.8/scala-2.11.8.rpm
sudo yum install scala-2.11.8.rpm
sudo yum install centos-release-scl-rh
sudo yum install python27
sudo scl enable python27 bash
scl enable python27 bash

scala -version
python -V

wget https://github.com/agus100cia/spark/blob/master/spark-on-windows.md#:~:text=https%3A//archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz
tar zxvf spark-2.4.5-bin-hadoop2.7.tgz 




sudo nano ~/.bash_profile
export SPARK_HOME=/opt/spark-2.4.5-bin-hadoop2.7
export PATH=$PATH:$SPARK_HOME/bin


``` 
