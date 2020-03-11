from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import row_number,lit,rank
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.functions import trim
from pyspark.sql.functions import count, avg, sum, max, min
from pyspark.sql.functions import udf
from bisect import bisect_right
from pyspark.sql.functions import col, expr, when
from pyspark.sql.functions import substring
import pyspark.sql.functions as func
import argparse
import bisect
import time
import datetime
from pyspark.sql.functions import lit,unix_timestamp

##CURRENT_TIMESTAMP para agregar en la fecha de proceso
timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
##Inicia la variable de entrada
vFechaDesde=20200101

##Captura de parametros
parser = argparse.ArgumentParser()
parser.add_argument("--ngrams", help="some useful description.")
args = parser.parse_args()
if args.ngrams:
    ngrams = args.ngrams
    vFechaDesde = ngrams

print("********************************************PARAMETROS DE ENTRADA***************************************")
print("vFechaDesde=" + vFechaDesde)
print("********************************************INICIO SPARK CONTEXT***************************************")

vConf = SparkConf().\
    setAppName("etl_py_prl_otc_t_salients_competencia_td").\
    set("spark.speculation","true")
sc = SparkContext(conf=vConf)
sc.setLogLevel("ERROR")

hc = HiveContext(sc)
vSQL_vwcelda1 =  "select " \
                 "celda_id as celda_id, " \
                 "ubicacion_provincia as celdacd " \
                 "from db_desarrollo.otc_t_celdas_unicas_parquet "

## db_desarrollo.otc_t_vwotc_spn_numeration_parquet
## db_cs_altas.otc_t_vwotc_spn_numeration
vSQL_TD_vwotc_spn_numeration ="SELECT  " \
                              "TA.msisdn, " \
                              "TA.receiverop, " \
                              "TA.fvc " \
                              "FROM (" \
                              "select " \
                              "ROW_NUMBER () over(PARTITION by msisdn order by fvc desc) as id, " \
                              "msisdn,  " \
                              "receiverop,  " \
                              "fvc " \
                              "from db_desarrollo.otc_t_vwotc_spn_numeration_parquet  " \
                              "where {}>=date_format(fvc,'yyyyMMdd') " \
                              ") " \
                              "TA WHERE  TA.id=1".format(vFechaDesde)

### db_desarrollo.otc_t_cppint_rango_nacional_parquet
### db_cppicx.otc_t_cppint_rango_nacional
vSQL_TD_OTC_T_RANGO_NACIONAL = "select " \
                               "codigo_operadora, " \
                               "inicio_rango, " \
                               "fin_rango " \
                               "from db_cppicx.otc_t_cppint_rango_nacional "
##db_desarrollo.otc_t_bloqueo_voz_parquet
##db_emm.otc_t_bloqueo_voz
vSQL_TD_vwXDRCursado = "select " \
                       "numero_a 							as celular, " \
                       "celda_destino 						as celda_id, " \
                       "duracion_real_llamada  				as duracion, " \
                       "fecha_llamada 						as fecha, " \
                       "cast(hora_llamada as varchar(8)) 	as hora, " \
                       "tipo_servicio 						as tipo_trafico, " \
                       "sentido_del_trafico 				as sentido, " \
                       "imsi_a 								as imsi " \
                       "from db_emm.otc_t_bloqueo_voz " \
                       "where " \
                       "fecha={} " \
                       "AND fecha_llamada = {} " \
                       "AND codigo_fuente = 37 " \
                       "AND duracion_real_llamada > 0 " \
                       "AND numero_b IS NOT NULL " \
                       "AND sentido_del_trafico = 'E' " \
                       "AND tipo_llamada = 'MO' " \
                       "AND length(Numero_a) = 9".format(vFechaDesde,vFechaDesde)


TD_vwXDRCursado = hc.sql(vSQL_TD_vwXDRCursado)
vwcelda1 = hc.sql(vSQL_vwcelda1)
TD_vwotc_spn_numeration = hc.sql(vSQL_TD_vwotc_spn_numeration)
TD_OTC_T_RANGO_NACIONAL = hc.sql(vSQL_TD_OTC_T_RANGO_NACIONAL)

##Presenta en pantalla el numero de registros de entrada
sqlContext = SQLContext(sc)
dfDataIn = sqlContext.createDataFrame(
    [
        ('TD_vwXDRCursado', hc.sql(vSQL_TD_vwXDRCursado).count()),
        ('vwcelda1', hc.sql(vSQL_vwcelda1).count()),
        ('TD_vwotc_spn_numeration', hc.sql(vSQL_TD_vwotc_spn_numeration).count()),
        ("TD_OTC_T_RANGO_NACIONAL", hc.sql(vSQL_TD_OTC_T_RANGO_NACIONAL).count())
    ],
    ["Fuente","Rows"]
)

dfDataIn.show()

print("**********************************************INICIO ETL***********************************************************")
##Transformer_191
def ttlookup(s):
    if (s==4) :
        return 'OFFNET'
    else:
        return 'OTRO'

ttlookup_udf = udf(ttlookup)
DSLink193 = TD_vwXDRCursado.withColumn(
    "tipo_trafico",
    ttlookup_udf("tipo_trafico")
).select(
    col("celular"),
    col("celda_id"),
    col("duracion"),
    col("fecha"),
    col("hora"),
    col("tipo_trafico"),
    col("sentido"),
    col("imsi")
)

## Join 195
DSLink231 = DSLink193.join(
                vwcelda1,
                DSLink193["celda_id"] == vwcelda1["celda_id"],
                how="left_outer"
               ).select (
                    DSLink193["celular"].alias("celular"),
                    DSLink193["celda_id"].alias("celda_id"),
                    DSLink193["duracion"].alias("duracion"),
                    DSLink193["fecha"].alias("fecha"),
                    DSLink193["hora"].alias("hora"),
                    DSLink193["tipo_trafico"].alias("tipo_trafico"),
                    DSLink193["sentido"].alias("sentido"),
                    vwcelda1["celdacd"].alias("celdacd"),
                    DSLink193["imsi"].alias("imsi")
                )

## Filter 234
DSLink236 =DSLink231.where("imsi like '74002%'  or imsi like '74001%'")
DSLink246 = DSLink231.where("imsi not like '74002%'  and  imsi not like '74001%'")

## Transformer 287
DSLink250 = DSLink246.\
    withColumn("celular", trim(col("celular").cast("string"))).\
    withColumn("celda_id", col("celdacd")).\
    select(
    "celular",
    "celda_id",
    "duracion",
    "fecha",
    "hora",
    "tipo_trafico",
    "sentido",
    "imsi"
    )

## Transformer 158
DSLink160 = TD_vwotc_spn_numeration\
    .withColumn(
    "msisdn",
    trim(regexp_replace(col("msisdn")," ",""))
    ).withColumn(
    "receiverop",
    col("receiverop").cast("string")
    ).select(
    col("msisdn"),
    col("receiverop").alias("operadora_a")
)


## Lookup_251
DSLink160Map = DSLink160.select("msisdn", "operadora_a").collect()
DSLink160Dic = dict(DSLink160Map)
def cellookup1(x):
    return DSLink160Dic.get(x)


lookup_udf_map1 = udf(cellookup1)
DSLink253 = DSLink250.withColumn(
    "operadora_a",
    lookup_udf_map1("celular")
)


Lnk_SI_OPERADORA = DSLink253.where(col("operadora_a").isNotNull())
Lnk_NO_OPERADORA = DSLink253.where(col("operadora_a").isNull())

## Transformer_148
DSLink149 = TD_OTC_T_RANGO_NACIONAL\
    .withColumn(
    "codigo_operadora",
    trim(col("codigo_operadora").cast("string"))
).withColumn(
    "inicio_rango",
    col("inicio_rango").cast("decimal")
).withColumn(
    "fin_rango",
    col("fin_rango").cast("decimal")
)

##Lookup_229
DSLink149Map = DSLink149.collect()
DSLink149SortInicio = DSLink149.select("inicio_rango","codigo_operadora").collect().sort()
DSLink149SortFin  = DSLink149.select("fin_rango","codigo_operadora").collect().sort()

def cellookup2(x):
    try:
        i = bisect.bisect_left(DSLink149SortInicio, x)
        j = bisect.bisect_right(DSLink149SortFin, x, lo=i)
        if j:
            return DSLink149Sort.value[i - 1]["operadora_a"]
        return None
    except:
        return "VACIO"



lookup_udf_map2=udf(cellookup2)
DSLink276 = Lnk_NO_OPERADORA.withColumn(
    "operadora_a",
    lookup_udf_map2("celular")
)


## Funnel_300
DSLink276A = Lnk_SI_OPERADORA.unionAll(DSLink276)

##Transformer_257
DSLink236_tmp = DSLink236.withColumn(
    "operadoraa_ss",
    substring('imsi', 1, 5)
)
DSLink261 = DSLink236_tmp.withColumn(
    "operadora_a",
    expr("""IF(operadoraa_ss = '74002', '2', '1')""")
).select(
    col("celular"),
    col("celda_id"),
    col("duracion"),
    col("fecha"),
    col("hora"),
    col("operadora_a")
)

## Transformer_258
DSLink260 = DSLink276A.withColumn(
    "operadora_a",
    expr("""IF(operadora_a = 40 OR operadora_a =4, 1, IF(operadora_a = 60 OR operadora_a = 11, 2, 3))""")
).select(
    col("celular"),
    col("celda_id"),
    col("duracion"),
    col("fecha"),
    col("hora"),
    col("operadora_a")
)

## Funnel_259
DSLink263 = DSLink260.unionAll(DSLink261)

## Copy_295
Lnk_Srt_Celda = DSLink263.select(
    col("celular"),
    col("celda_id"),
    col("duracion"),
    col("fecha"),
    col("operadora_a")
)

DSLink297 = DSLink263.select(
    col("celular"),
    col("celda_id"),
    col("duracion"),
    col("fecha"),
    col("hora"),
    col("operadora_a" )
)

## Copy_298
DSLink300 = DSLink297.select (
    col("fecha"),
    col("celular"),
    col("celda_id"),
    col("operadora_a"),
    col("duracion")
)

DSLink302 = DSLink297.select (
    col("fecha"),
    col("celular"),
    col("celda_id"),
    col("operadora_a")
)
DSLink304 = DSLink297.select (
    col("celular"),
    col("celda_id"),
    col("fecha"),
    col("hora"),
    col("operadora_a")
)
DSLink306 = DSLink297.select (
    col("celular"),
    col("celda_id"),
    col("fecha"),
    col("hora"),
    col("operadora_a")
)
DSLink308 = DSLink297.select (
    col("celular"),
    col("fecha"),
    col("operadora_a"),
    col("hora")
)


## Aggregator_309
DSLink311 = DSLink300\
    .groupBy(
        "fecha",
        "celular",
        "celda_id",
        "operadora_a")\
    .agg(
        sum("duracion")
            .alias("duracion")
    )


##Aggregator_312
DSLink314 = DSLink302\
    .groupBy(
            "fecha",
            "celular",
            "celda_id",
            "operadora_a")\
    .agg(
            count(func.lit(1))
                .alias("eventos")
    )


## Aggregator_315
DSLink317 = DSLink308\
    .groupBy(
            "fecha",
            "celular",
            "operadora_a")\
    .agg(
            max("hora").alias("hora_max"),
            min("hora").alias("hora_min")
    )


##Transformer_318
DSLink320 = DSLink317.select(
    col("fecha"),
    col("celular"),
    col("operadora_a"),
    col("hora_max").alias("hora")
)

DSLink322 = DSLink317.select(
    col("fecha"),
    col("celular"),
    col("operadora_a"),
    col("hora_min").alias("hora")
)

##Join_326
DSLink328 = DSLink304.join(
                            DSLink320,
                            (DSLink304["fecha"].alias("fecha_j") == DSLink320["fecha"]) &
                            (DSLink304["celular"].alias("celular_j") == DSLink320["celular"]) &
                            (DSLink304["operadora_a"].alias("operadora_a_j") == DSLink320["operadora_a"]) &
                            (DSLink304["hora"].alias("hora_j") == DSLink320["hora"]),
                            how = "inner"
                           ).select(
                                DSLink304["celular"],
                                DSLink304["celda_id"],
                                DSLink304["fecha"],
                                DSLink304["hora"],
                                DSLink304["operadora_a"]
                            )


## Join_329
DSLink331 =  DSLink306.join(
                            DSLink322,
                            (DSLink306["fecha"].alias("fecha_j") == DSLink322["fecha"] ) &
                            (DSLink306["celular"].alias("celular_j") == DSLink322["celular"]) &
                            (DSLink306["operadora_a"].alias("operadora_a_j") == DSLink322["operadora_a"]) &
                            (DSLink306["hora"].alias("hora_j") == DSLink322["hora"]) ,
                            how = "inner"
                            ).select(
                                DSLink306["celular"],
                                DSLink306["celda_id"],
                                DSLink306["fecha"],
                                DSLink306["hora"].alias("horami"),
                                DSLink306["operadora_a"]
                            )


## Join_323
DSLink325 = DSLink311.join(
                        DSLink314,
                        (DSLink311["celular"].alias("celular_j2") == DSLink314["celular"]) &
                        (DSLink311["celda_id"].alias("celda_id_j2") == DSLink314["celda_id"]) &
                        (DSLink311["operadora_a"].alias("operadora_a_j2") == DSLink314["operadora_a"]),
                        how = "inner"
                    ).select(
                        DSLink311["fecha"],
                        DSLink311["celular"],
                        DSLink311["celda_id"],
                        DSLink311["operadora_a"],
                        DSLink311["duracion"],
                        DSLink314["eventos"]
                    )


##Merge_332
DSLink325_tmp = DSLink325.\
    withColumnRenamed('fecha','fecha_x').\
    withColumnRenamed('celular','celular_x').\
    withColumnRenamed('celda_id','celda_id_x').\
    withColumnRenamed('operadora_a','operadora_a_x').\
    withColumnRenamed('duracion','duracion_x').\
    withColumnRenamed('eventos','eventos_x')

DSLink328_tmp = DSLink328. \
    withColumnRenamed('celular', 'celular_y'). \
    withColumnRenamed('celda_id', 'celda_id_y'). \
    withColumnRenamed('fecha', 'fecha_y'). \
    withColumnRenamed('hora', 'hora_y'). \
    withColumnRenamed('operadora_a', 'operadora_a_y')


DSLink259_1_tmp = DSLink325_tmp.join(
    DSLink328_tmp,
    (DSLink325_tmp["fecha_x"] == DSLink328_tmp["fecha_y"]) &
    (DSLink325_tmp["celular_x"] == DSLink328_tmp["celular_y"]) &
    (DSLink325_tmp["celda_id_x"] == DSLink328_tmp["celda_id_y"]) &
    (DSLink325_tmp["operadora_a_x"] == DSLink328_tmp["operadora_a_y"]),
    how="inner"
)

DSLink331_tmp = DSLink331. \
    withColumnRenamed('celular', 'celular_z').\
    withColumnRenamed('celda_id', 'celda_id_z'). \
    withColumnRenamed('fecha', 'fecha_z'). \
    withColumnRenamed('horami', 'horami_z'). \
    withColumnRenamed('operadora_a', 'operadora_a_z')

DSLink259_1 = DSLink259_1_tmp.join(
                                    DSLink331_tmp,
                                    (DSLink259_1_tmp["fecha_x"] == DSLink331_tmp["fecha_z"]) &
                                    (DSLink259_1_tmp["celular_x"] == DSLink331_tmp["celular_z"]) &
                                    (DSLink259_1_tmp["celda_id_x"] == DSLink331_tmp["celda_id_z"]) &
                                    (DSLink259_1_tmp["operadora_a_x"] == DSLink331_tmp["operadora_a_z"]),
                                    how = "inner"
).select(
    col("fecha_x").alias("fecha"),
    col("celular_x").alias("celular"),
    col("celda_id_x").alias("celda_id"),
    col("operadora_a_x").alias("operadora_a"),
    col("duracion_x").alias("duracion"),
    col("eventos_x").alias("eventos"),
    col("hora_y").alias("hora"),
    col("horami_z").alias("horami")
)

## Transformer_335
DSLink259_tmp = DSLink259_1.\
    withColumn("celda_id",
               when(
                   (col("celda_id").isNotNull()) |
                   (col("celda_id") !=''),
                   col("celda_id")
               ).otherwise('Sin Celda')
               ).\
    withColumn("evento_inicial",
               when(
                    (col("horami").isNotNull()) |
                    (col("horami") != ''),
                   'SI'
               ).otherwise('NO')
               ).\
    withColumn(
                "evento_final",
                when(
                    (col("hora").isNotNull()) |
                    (col("hora")!=''),
                    'SI'
                ).otherwise('NO')
            ).\
    withColumn("operadora",
               when(
                    col("operadora_a") == 1,
                   'Claro'
               ).otherwise('Cnt')
               )



DSLink259 = DSLink259_tmp.select(
    col("fecha"),
    col("celular"),
    col("celda_id"),
    col("duracion"),
    col("eventos"),
    col("evento_inicial"),
    col("evento_final"),
    col("horami").alias("hora_evento_inicial"),
    col("hora").alias("hora_evento_final"),
    col("operadora")
)


DSLink259Hive = DSLink259.\
    withColumn("fecha_carga", unix_timestamp(lit(timestamp),'yyyy-MM-dd HH:mm:ss').cast("timestamp")).\
    withColumn("fecha_proceso", lit(vFechaDesde))






## Agg_Duracion
Lnk_Agg_Duracion = Lnk_Srt_Celda.groupBy(
        "celular",
        "celda_id",
        "fecha",
        "operadora_a")\
    .agg(
        sum("duracion")
            .alias("duracion")
    )



## Rm_Celda
DSLink150 = Lnk_Agg_Duracion.\
    orderBy("celular", "fecha", "celda_id", "operadora_a").\
    dropDuplicates(subset = ['celular','fecha','celda_id','operadora_a'])

## Transformer_151
from pyspark.sql.functions import lit,unix_timestamp
from pyspark.sql import functions as F

Lnk_Rm_Celda = DSLink150.withColumn("celda_id",
                                    when(
                                        (col("celda_id") == "") |
                                        (col("celda_id") == '-1'),
                                        '-1000'
                                    ).otherwise(col("celda_id"))
                                    ).select(
                                        col("celular"),
                                        col("celda_id"),
                                        col("duracion"),
                                        col("fecha"),
                                        col("operadora_a").alias("operadoraid"),
                                        F.current_timestamp().alias("fecha_proceso")
                                    )


Lnk_Rm_CeldaHive = Lnk_Rm_Celda.\
    withColumn("fecha_carga", unix_timestamp(lit(timestamp),'yyyy-MM-dd HH:mm:ss').cast("timestamp")).\
    withColumn("fecha_proceso", lit(vFechaDesde))


##saveAsTable("db_desarrollo.OTC_T_COMSALIENTE1", format='parquet')

DSLink279 = DSLink150.select(
    col("celular"),
    col("celda_id"),
    col("fecha"),
    col("operadora_a"),
    col("duracion")
)




##Write
hc.sql("drop table if exists db_desarrollo.OTC_T_SAL_COM_DETALLE_tmp")
DSLink259Hive.saveAsTable("db_desarrollo.OTC_T_SAL_COM_DETALLE_tmp")

#create table db_desarrollo.OTC_T_SAL_COM_DETALLE (
#fecha			date,
#celular			int,
#celda_id		varchar(20),
#duracion		int,
#eventos			int,
#eventos_inicial	varchar(20),
#eventos_final	varchar(20),
#hora_evento_inicial	varchar(8),
#hora_evento_final	varchar(8),
#operadora			varchar(20),
#fecha_carga			timestamp
#)
#partitioned by (fecha_proceso int)
#stored as parquet;

vSQL_DSLink259Hive = "insert overwrite table db_desarrollo.OTC_T_SAL_COM_DETALLE partition (fecha_proceso={}) " \
                     "select  " \
                     "date_format(from_unixtime(unix_timestamp(cast(fecha as string),'yyyyMMdd')),'yyyy-MM-dd') as fecha, " \
                     "celular, " \
                     "celda_id, " \
                     "duracion, " \
                     "eventos, " \
                     "evento_inicial, " \
                     "evento_final, " \
                     "hora_evento_inicial, " \
                     "hora_evento_final, " \
                     "operadora, " \
                     "fecha_carga " \
                     "from db_desarrollo.OTC_T_SAL_COM_DETALLE_tmp " \
                     "where  " \
                     "fecha_proceso={} ".format(vFechaDesde, vFechaDesde)
hc.sql(vSQL_DSLink259Hive)
###hc.sql("drop table if exists db_desarrollo.OTC_T_SAL_COM_DETALLE_tmp")



hc.sql("drop table if exists db_desarrollo.OTC_T_COMSALIENTE1_tmp")
Lnk_Rm_CeldaHive.saveAsTable("db_desarrollo.OTC_T_COMSALIENTE1_tmp")
##CREATE TABLE db_desarrollo.OTC_T_COMSALIENTE1 (
#celular			int,
#celda_id		varchar(20),
#duracion		int,
#fecha			date,
#operadoraid		varchar(5),
#fecha_proceso	date,
#fecha_carga		timestamp
##)
##partitioned by (fechaproceso int)
##stored as parquet;

##Mueve los datos del temporal a la tabla particionada
vSQL_Lnk_Rm_CeldaHive = "insert overwrite table db_desarrollo.OTC_T_COMSALIENTE1 " \
       "partition (fechaproceso={} ) " \
       "select  " \
       "celular	, " \
       "celda_id, " \
       "duracion, " \
       "date_format(from_unixtime(unix_timestamp(cast(fecha as string),'yyyyMMdd')),'yyyy-MM-dd') as fecha, " \
       "operadoraid, " \
       "date_format(from_unixtime(unix_timestamp(cast(fecha_proceso as string),'yyyyMMdd')),'yyyy-MM-dd') as fecha_proceso, "\
       "fecha_carga " \
       "from db_desarrollo.OTC_T_COMSALIENTE1_tmp " \
       "where fecha_proceso={} ".format(vFechaDesde, vFechaDesde)
hc.sql(vSQL_Lnk_Rm_CeldaHive)
##hc.sql("drop table if exists db_desarrollo.OTC_T_COMSALIENTE1_tmp")



## Write File
import numpy as np
import pandas as pd
DSLink279Pandas = DSLink279.toPandas()
DSLink279Pandas.to_csv('salientes_comp.txt', index=False, encoding='utf-8')



dfDataOut = sqlContext.createDataFrame(
    [
        ('OTC_T_SAL_COM_DETALLE', DSLink259Hive.count()),
        ('OTC_T_COMSALIENTE1', Lnk_Rm_CeldaHive.count()),
        ('salientes_comp.txt', DSLink279.count())
    ],
    ["Resultado","Rows"]
)
dfDataOut.show()