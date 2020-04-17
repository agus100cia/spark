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
