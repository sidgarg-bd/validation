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

    val someData = Seq(
      Row(1, "8", "bat"),
      Row(2, null, null),
      Row(3, null, "cat"),
      Row(4, "", ""),
      Row(null, "27", "horse"),
      Row(6, "", null)

    )

    val someSchema = List(
      StructField("id", IntegerType, true),
      StructField("class", StringType, true),
      StructField("word", StringType, true)
    )

    val someDF = spark.createDataFrame(
      sc.parallelize(someData),
      StructType(someSchema)
    )

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


    lengthCheck(someDF, "word", 3, true)
    lengthCheck(someDF, "word", 3, false)
    println("Bye from this App")
  }
}

