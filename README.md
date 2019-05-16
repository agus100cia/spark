# spark
Notas de desarrollo en spark

Dado que el desarrollo BigData para américa latina es bastante limitado, es difícil encontrar fuentes de información o pedazos de código que nos ayuden a desarrollar programas de análisis de datos con spark en español.

Por eso he decidido crear este repositorio para ir agregando mis experiencias con spark en sus distintas versiones.

Espero les sirva tanto como a mí

## CASO DE USO SPARK

Felipao gracias por tu ayuda. 

Te doy una pequeña introduccion a lo que es SPARK.

Spark es un framework de procesamiento distribuido, ahorita vas a ejecutar spark sobre un cluster de 5 servidores, Spark controla internamente el paralelizmo de los procesos y los try/catch en caso de que un servidor deje de funcionar, básicamente tu debes centrarte en el código.

SPARK puedes programar en JAVA, SCALA, R, PYTHON. En este IDE (Zeppelin), he configurado para programar en Scala, Python o R.
Lo que debes hacer es crear un párrafo y antes de nada poner el tag del lenguaje en el que vas a programar. Ejemplo

Scala = %spark
Python = %pyspark


En spark se maneja el concepto de RDD, esto es como decir una tabla en una base de datos, basicamente es una matriz de datos sobre el cual puedes ejecutar procesos en forma de transformaciones y acciones. 
Las transformaciones es decirle a spark lo que tiene que hacer
Las acciones es cuando spark ya lo hace.

Te voy a dar 2 RDD

rdd test  El objetivo es consultar el nombre padre en el RDD rdd catalogo y obtener la cedula en los casos que coincida
```
-----------------------------------------------------------------------------------------------------
cedula      |nombre                               |cedulapadre  |nombrepadre                     
-----------------------------------------------------------------------------------------------------
1803624327	|GUAYGUA REYES MARIA CRISTINA	 	  |              |GUAYGUA TONATO LUIS HERNAN
1803624525	|BARROSO JINEZ MONICA DE LOS ANGELES  |	 	         |BARROSO AMAN LUIS ERNESTO
1803599750	|DIAZ CONTERON DANIELA ALEXANDRA      |	 	         |ALBERTO DIAZ
1803599826	|CHACHA GUANGASI MARIA PIEDAD	 	  |              |**********************
1803599917	|VELASTEGUI LUZURIAGA MONICA ISABEL	  | 	         |VELASTEGUI ANDALUZ RODRIGO ABEL
-----------------------------------------------------------------------------------------------------
```

rdd_catalogo = Tiene un listado de cedulas y nombres
```
-----------------------------------------------------------------------------------------------------
cedula      |nombre                    
-----------------------------------------------------------------------------------------------------
0650531700	|CHAFLA GUAMAN JONATAN ALEJANDRO
0650531718	|HIDALGO TOLEDO ANAELA SALOME
1801188747	|GUAYGUA TONATO LUIS HERNAN
1600078586	|BARROSO AMAN LUIS ERNESTO
1801032010	|VELASTEGUI ANDALUZ RODRIGO ABEL
-----------------------------------------------------------------------------------------------------
```

Para esto considero que puedes usar 2 funciones map o faltMap y filter.

MAP o FLATMAP: Te permite aplicar una funcion por cada registro del RDD
FILTER: Es como hacer un where en una tabla SQL

Te dejo un par de ejemplos en scala:

MAP:

````scala

val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split(" "))
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)
counts.saveAsTextFile("hdfs://...");

Te dejo el link oficial:

https://spark.apache.org/examples.html

````


