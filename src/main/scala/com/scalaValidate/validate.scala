package com.scalaValidate

import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object validate {
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

    def Check(df: DataFrame, ls: List[String], condition: String, sze: Int = 4, ignore: Boolean): Unit = {
      val newDf = df.select(ls.map(col): _*)
      if (ignore == true) {
        val res = ignoreTrue(newDf: DataFrame)
      }
      else {
        if (condition == "null") {
          val res = nullCheck(newDf: DataFrame)
        }
        else if (condition == "notnull") {
          val res = notNullCheck(newDf: DataFrame)
        }
        else if (condition == "empty") {
          val res = emptyCheck(newDf: DataFrame)
        }
        else if (condition == "length") {
          if (ls.length == 1) {
            val res = lengthCheck(newDf: DataFrame, ls: List[String], sze: Int)
          }
          else {
            println("Column list for length check must be 1..EXITING")
            System.exit(1)
          }
        }
      }

    }

    def ignoreTrue(frame: DataFrame): Unit = {
      println("Ignore True case: Filter Not Null Values")
      frame.na.drop().show()
    }

    def nullCheck(frame: DataFrame): Unit = {
      val filterCond = frame.columns.map(x => col(x).isNull).reduce(_ || _)
      println("Ignore False NULL case: Filter Null Values as true")
      val filteredDf = frame.withColumn("result", when(filterCond, true).otherwise(false)).show()
    }

    def notNullCheck(frame: DataFrame): Unit = {
      val filterCond = frame.columns.map(x => col(x).isNotNull).reduce(_ && _)
      println("Ignore False Not NULL case: Filter Not Null Values as true")
      val filteredDf = frame.withColumn("result", when(filterCond, true).otherwise(false)).show()
    }

    def emptyCheck(frame: DataFrame): Unit = {
      val filterCond = frame.rdd.isEmpty()
      println("Ignore False empty case: check if dataframe is empty or not")
      if (filterCond == true) {
        println("Dataframe is Empty")
      }
      else {
        println("Dataframe is not empty")
      }
    }

    def lengthCheck(frame: DataFrame, lst: List[String], sz: Int): Unit = {
      for (colName <- lst) {
        val filteredDf = frame.filter(length(col(colName)) === sz)
        if (filteredDf.count() > 0) {
          println("Dataframe's column length matched")
        }
        else {
          println("Dataframe's column length not matched")
        }
      }
    }

    Check(someDF, List("number", "word"), "null", 1, true)
    Check(someDF, List("number", "word"), "null", 1, false)
    Check(someDF, List("number", "word"), "notnull", 1, false)
    Check(someDF, List("number", "word"), "empty", 1, false)
    Check(someDF, List("word"), "length", 3, false)
    println("Bye from this App")
  }
}

