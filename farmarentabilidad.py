from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.functions import lit,unix_timestamp
import datetime
import time

import argparse


## Lectura de parametros
parser = argparse.ArgumentParser()
parser.add_argument('--anio', required=True, type=int)
parser.add_argument('--mes', required=True, type=int)
parametros = parser.parse_args()

vanio=parametros.anio
vmes=parametros.mes

timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

##SPARK
vConf = SparkConf().\
    setAppName("pyspark_farmarentabilidad_{anio}_{mes}".format(anio=vanio,mes=vmes)).\
    set("spark.speculation","true")
sc = SparkContext(conf=vConf)

hc = HiveContext(sc)

vSQLFarmaComercial = "SELECT " \
                     "anio, " \
                     "mes, " \
                     "codvendedor, " \
                     "codoficina, " \
                     "codcliente, " \
                     "fechafactura, " \
                     "dia, " \
                     "seriefactura, " \
                     "codproducto, " \
                     "valorventa, " \
                     "cantlaboratorio, " \
                     "cantidadentregada, " \
                     "cantidadventa " \
                     "FROM dwh.farmacomercial " \
                     "WHERE " \
                     "anio={anio} " \
                     "and " \
                     "mes={mes} " \
                     "and " \
                     "codtipoventa='B' ".format(anio=vanio, mes=vmes)

dfComercial = hc.sql(vSQLFarmaComercial)


vSQLFarmaCostos = "SELECT " \
                        "anio, " \
                        "mes, " \
                        "codproducto, " \
                        "seriefactura, " \
                        "costoinventario " \
                        "FROM dwh.farmacostos " \
                        "WHERE " \
                        "anio={anio} " \
                        "and " \
                        "mes={mes} ".format(anio=vanio,mes=vmes)

dfCosto = hc.sql(vSQLFarmaCostos)

dfComercialGrp = dfComercial.groupBy(
        "anio",
        "mes",
        "codvendedor",
        "codoficina",
        "codcliente",
        "fechafactura",
        "dia",
        "seriefactura",
        "codproducto"
    ).agg(
        sum("valorventa").alias("valorventa"),
        sum("cantlaboratorio").alias("cantlaboratorio"),
        sum("cantidadventa").alias("cantidadventa")
        )

dfCostoGrp = dfCosto.groupBy(
        "anio",
        "mes",
        "seriefactura",
        "codproducto"
    ).agg(
        sum("costoinventario").alias("costoinventario")
        )

dfRentabilidad0 = dfComercialGrp.join(
    dfCostoGrp,
    (dfComercialGrp["anio"]==dfCostoGrp["anio"]) &
    (dfComercialGrp["mes"]==dfCostoGrp["mes"]) &
    (dfComercialGrp["seriefactura"]==dfCostoGrp["seriefactura"]) &
    (dfComercialGrp["codproducto"]==dfCostoGrp["codproducto"])
    ).select(
        dfComercialGrp["anio"],
        dfComercialGrp["mes"],
        dfComercialGrp["codvendedor"],
        dfComercialGrp["codoficina"],
        dfComercialGrp["codcliente"],
        dfComercialGrp["fechafactura"],
        dfComercialGrp["dia"],
        dfComercialGrp["seriefactura"],
        dfComercialGrp["codproducto"],
        dfComercialGrp["valorventa"],
        dfComercialGrp["cantlaboratorio"],
        dfComercialGrp["cantidadventa"],
        dfCostoGrp["costoinventario"]
        )

dfRentabilidad1 = dfRentabilidad0.withColumn(
    "utilidadventa",
    col("valorventa") - col("costoinventario")
).withColumn(
    "fechacarga",
    lit(timestamp)
)

dfRentabilidad2 = dfRentabilidad1.select(
    "anio",
    "mes",
    "codcliente",
    "codoficina",
    "codvendedor",
    "fechafactura",
    "dia",
    "seriefactura",
    "codproducto",
    "cantlaboratorio",
    "cantidadventa",
    "valorventa",
    "costoinventario",
    "utilidadventa",
    "fechacarga"
)

vSQLDeletePartition = "ALTER TABLE dwh.farmarentabilidad DROP IF EXISTS PARTITION(anio={anio}, mes={mes})".format(anio=vanio,mes=vmes)
hc.sql(vSQLDeletePartition)

dfRentabilidad2.coalesce(2).write.mode("append").partitionBy('anio', 'mes').saveAsTable("dwh.farmarentabilidad")

print("Listo Anio={anio} Mes={mes}".format(anio=vanio, mes=vmes))
