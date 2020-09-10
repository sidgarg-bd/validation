package com.scalaValidate

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object lengthValidate {
  def main(args: Array[String]) {
    println("Hello from App")
    val conf = new SparkConf()
      .setAppName("POC")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val someDF = spark.read.format("csv").option("header", "true").option("delimiter","|").load("src/main/resources/test.csv")

    def lengthCheck(df: DataFrame, colName: String, sze: Int, ignore: Boolean): Unit =
    {
      if (ignore == true) {
        val res = ignoreTrue(df: DataFrame, colName: String, sze: Int)
      }
      else {
        val res = ignoreFalse(df: DataFrame, colName: String, sze: Int)
      }
    }

    def ignoreTrue(frame: DataFrame, colNam: String, sz: Int): Unit = {
      val newLenFrame = frame.filter(length(col(colNam)) === sz).show()
      println("Ignore True case: Filter Values with Length")
    }

    def ignoreFalse(frame: DataFrame, colNam: String, sz: Int): Unit = {
      val resDf = frame.withColumn("result", when(length(col(colNam)) === sz, true).otherwise(false)).show()
      println("Ignore False Length check case: True for correct length values in list")
    }


    lengthCheck(someDF, "agcy_id", 4, true)
    lengthCheck(someDF, "agcy_id", 4, false)
    println("Bye from this App")
  }
}

