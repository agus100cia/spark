# Encriptar una o varias columnas

## Generar la llave criptográfica

Lo primero es crear una llave privada para poder encriptar o desencriptar los datos. Para esto vamos a necesitar una libreria de Python llamada "cryptography"

Instalar la libreria en Python

```sh
pip install cryptography

```

Para poder crear la clave en base a una frase o password.

```python
import base64
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

password_provided = "clavesecreta" # This is input in the form of a string
password = password_provided.encode() # Convert to type bytes
salt = b'salt_' # CHANGE THIS - recommend using a key from os.urandom(16), must be of type bytes
##salt = os.urandom(16)
kdf = PBKDF2HMAC(
    algorithm=hashes.SHA256(),
    length=32,
    salt=salt,
    iterations=100000,
    backend=default_backend()
)
key = base64.urlsafe_b64encode(kdf.derive(password)) # Can only use kdf once


file = open('/Users/agus/PycharmProjects/cryptokey/aguskey.key', 'wb')
file.write(key) # The key is type bytes still
file.close()

``` 

## Encriptar y desencriptar columna en base a una llave (keyfile)

```python

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
from cryptography.fernet import Fernet
import base64
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

vGlobalKey = ""

def getDataFrame(hc):
    vSqlTabla = "select " \
                  "idpersona, " \
                  "codpersona, " \
                  "nombre, " \
                  "edad, " \
                  "telefono " \
                  "from esquema.tabla " \
                  "limit 3"
    df = hc.sql(vSqlTabla)
    return df


def getKey():
    vKeyFile = open("aguskey.key", "r")
    vKey = vKeyFile.read()
    return vKey


def desencriptarValor(valor):
    password_provided = vGlobalKey
    password = password_provided.encode()  # Convert to type bytes
    salt = b'salt_'  # CHANGE THIS - recommend using a key from os.urandom(16), must be of type bytes
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
        backend=default_backend()
    )
    key = base64.urlsafe_b64encode(kdf.derive(password))  # Can only use kdf once

    encrypted = valor
    f = Fernet(key)
    decrypted = f.decrypt(encrypted)
    return decrypted

def encriptarValor(valor):
    password_provided = vGlobalKey
    password = password_provided.encode()  # Convert to type bytes
    salt = b'salt_'  # CHANGE THIS - recommend using a key from os.urandom(16), must be of type bytes
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
        backend=default_backend()
    )
    key = base64.urlsafe_b64encode(kdf.derive(password))  # Can only use kdf once
    from cryptography.fernet import Fernet
    message = valor.encode()

    f = Fernet(key)
    encrypted = f.encrypt(message)
    return encrypted

def lookupudfdesencriptar(valor):
    desencrypted = desencriptarValor(valor)
    return desencrypted


def lookupudfencriptar(valor):
    encrypted = encriptarValor(valor)
    return encrypted


def getDfDesencriptado(df):
    lookup_udf_desencriptar = udf(lookupudfdesencriptar)
    dfDesencriptado = df.withColumn(
        "desencriptado",
        lookup_udf_desencriptar("encriptado")
    )
    return dfDesencriptado

def getDfEncriptado(df):
    lookup_udf_encriptar = udf(lookupudfencriptar)
    dfEncriptado = df.withColumn(
        "encriptado",
        lookup_udf_encriptar("codpersona")
    )
    return dfEncriptado

def main():
    ## leer la frase de la llave privada
    vKey = getKey()
    vGlobalKey = vKey

    vConf = SparkConf(). \
        setAppName("spark-app"). \
        setMaster("local[*]"). \
        set("spark.speculation", "true")
    sc = SparkContext(conf=vConf)
    sc.setLogLevel("ERROR")
    hc = HiveContext(sc)
    dfOriginal = getDataFrame(hc)
    dfEncriptado = getDfEncriptado(dfOriginal)
    dfEncriptado.show(3)
    dfDesencriptado = getDfDesencriptado(dfEncriptado)
    dfDesencriptado.show(3)




if __name__ == '__main__':
    main()

``` 
