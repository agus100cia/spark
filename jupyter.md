# Instalar Jupyter con Pyspark

## Jupyter con Pyspark en MAC

Debes tener instalador brew

```sh
xcode-select --install
xcode-select -p
brew update && brew doctor
brew install pyenv
## Selecciona la version a instalar
pyenv install --list | grep " 3\.[678]"
pyenv install 3.8.5
pyenv versions

``` 

Instalar  Jupyter

Al ejecutar "pyspark" se abrirá Jupyter

```sh
brew install jupyter
pip3 install jupyter
## Define variables de ambiente
export SPARK_HOME=/Users/agus/DocsLocal/spark/spark-2.4.5-bin-hadoop2.7
export PATH=$PATH:$SPARK_HOME/bin
export YARN_CONF_DIR=/Users/agus/DocsLocal/spark/spark-2.4.5-bin-hadoop2.7/conf
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
pyspark

```
