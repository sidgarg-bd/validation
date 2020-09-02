package com.scalaValidate

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object nullCheck {

  def main(args: Array[String]) {
    println("Hello from App")
    val conf = new SparkConf()
      .setAppName("POC")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    val someData = Seq(
      Row(8, "bat"),
      Row(null, "cat"),
      Row(-27, "horse")
    )

    val someSchema = List(
      StructField("number", IntegerType, true),
      StructField("word", StringType, true)
    )

    val someDF = spark.createDataFrame(
      sc.parallelize(someData),
      StructType(someSchema)
    )

    def Check(df: DataFrame, ls: List[String], condition: String): Unit = {
      if (condition == "null") {
        val res = nullCheck(df: DataFrame, ls: List[String])
        println(res)
      }
    }

    def nullCheck(frame: DataFrame, lst: List[String]): Boolean = {
      var check = 0
      var res = false
      for (colName <- lst) {
        if (frame.filter(col(colName).isNull).count() > 0) {
          check = check + 1
        }
      }
      if (check > 0) {
        res = true
      }
      return res
    }

    Check(someDF, List("number"), "null")
    println("Bye from this App")
  }
}
