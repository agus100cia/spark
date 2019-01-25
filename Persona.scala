package com.sparkTutorial.rdd

import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, _}
import java.util.List
import java.util.ArrayList

import java.util

import org.apache.spark
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType};



object Persona {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("persona").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val rdd_catalogo = sc.textFile("in/catalogo")
    val rdd_data = sc.textFile("in/rddtest")

    val strVal = new StringBuilder();
    for (line <- rdd_data.collect) {
      val splits = line.split("\t");
      var cedula = splits(0);
      var nombre = splits(1);
      var nompadre = splits(3);

      val rdd_filter = rdd_catalogo.filter(x => {x.split("\t")(1) == nompadre});
      var cedpadre = ""
      for (xl <- rdd_filter.collect) {
        cedpadre = xl.split("\t")(0);
      }
      var linea = cedula + "\t" + nombre + "\t" + nompadre + "\t" + cedpadre + "\n" ;
      strVal.append(linea)
      //System.out.println(linea)
    }

    val valSeq = Seq(strVal);
    val rdd_result=sc.parallelize(valSeq);

    rdd_result.saveAsTextFile("out/rddpadre");

  }
}
