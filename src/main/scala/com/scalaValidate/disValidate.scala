package com.scalaValidate

import org.apache.spark.sql.functions.{col, length, when}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object disValidate {

  def main(args: Array[String]) {
    try{
      println("Hello from App")
      val conf = new SparkConf()
        .setAppName("POC")
        .setMaster("local[2]")

      val sc = new SparkContext(conf)
      val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
      val someDF = spark.read.format("csv").option("header", "true").load("src/main/resources/test.csv")

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
        val filteredDf = cond.withColumn("result", when(col("count") > 1, true).otherwise(false)).drop("count")
        val joinCond = frame.columns.map(x=>frame.col(x) <=> filteredDf.col(x)).reduce(_ && _)
        var resDf = frame.join(filteredDf, joinCond)
        for(colName <- frame.columns){
          resDf = resDf.drop(frame.col(s"$colName"))
        }
        resDf.show()
        println("Ignore False Distinct check case: True for Duplicates")
      }

      disCheck(someDF, true)
      disCheck(someDF, false)
      println("Bye from this App")
    }

    catch{
      case x: AnalysisException => println(s"Analysis Exception: $x")
      case x: NullPointerException => println(s"Null Pointer Exception: $x")
      case unknown: Exception => println(s"Unknown Exception: $unknown")
    }
  }
}
