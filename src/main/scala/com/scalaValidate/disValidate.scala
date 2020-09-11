package com.scalaValidate

import org.apache.spark.sql.functions.{col, length, when}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object disValidate {

  def main(args: Array[String]) {
    println("Hello from App")
    val conf = new SparkConf()
      .setAppName("POC")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    val someData = Seq(
      Row(8, "bat"),
      Row(8, "bat"),
      Row(-27, "horse"),
      Row(7, "cat"),
      Row(7, "cat")
    )

    val someSchema = List(
      StructField("number", IntegerType, true),
      StructField("word", StringType, true)
    )

    val someDF = spark.createDataFrame(
      sc.parallelize(someData),
      StructType(someSchema)
    )

    def disCheck(df: DataFrame, ignore: Boolean): Unit = {
      if (ignore == true) {
        val res = ignoreTrue(df: DataFrame)
      }
      else {
        val res = ignoreFalse(df: DataFrame)
      }
    }

    def ignoreTrue(frame: DataFrame): Unit = {
      val newDisFrame = frame.dropDuplicates().show()
      println("Ignore True case: Filter Duplicates")
    }

    def ignoreFalse(frame: DataFrame): Unit = {
      val cond: DataFrame = frame.groupBy(frame.columns.map(col):_*).count()
      val filteredDf = cond.withColumn("result", when(col("count") > 1, true).otherwise(false))
      filteredDf.drop("count").show()
      println("Ignore False Length check case: True for correct length values in list")
    }

    disCheck(someDF, true)
    disCheck(someDF, false)
    println("Bye from this App")

  }
}
