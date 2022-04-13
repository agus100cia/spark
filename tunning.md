### Lectura 

Por defecto Spark lee 10 filas en batch, para aumentar la velocidad de lectura aumenta el parámetro "fetchsize" por iteración.

```py
data=spark.read.format("jdbc")
.option("url",tns_path)
.option("dbtable",query)
.option("user",userid)
.option("password",password)
.option("fetchsize","100000")
.option("driver","oracle.jdbc.driver.OracleDriver")
.load()

``` 

### Escritura

Por lote de escritura aumenta el bloque de registros con el parámetro "batchsize"

```py

data.write.format("jdbc")
.option("url",tns_path)
.option("dbtable",schemaname.tablename)
.option("user",userid)
.option("password",password)
.option("fetchsize","100000")
.option("driver","oracle.jdbc.driver.OracleDriver")
.option("batchsize","100000")
.mode('append').save()

``` 
