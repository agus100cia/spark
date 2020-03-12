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
