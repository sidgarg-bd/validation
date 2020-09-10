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

    def Check(df: DataFrame, ls: List[String], condition: String, ignore: Boolean): Unit =
    {
      val newDf = df.select(ls.map(col): _*)
      if(ignore == true)
      {
        val res = ignoreTrue(df: DataFrame, newDf: DataFrame)
      }
      else
      {
        if (condition == "null")
        {
          val res = nullCheck(df: DataFrame, newDf: DataFrame)
        }
        else if(condition == "notnull")
        {
          val res = notNullCheck(df: DataFrame, newDf: DataFrame)
        }
        else if (condition == "empty")
        {
          val res = emptyCheck(df: DataFrame, newDf: DataFrame)
        }
      }
    }

    def ignoreTrue(frame: DataFrame, newFrame: DataFrame): Unit = {
      val newDropFrame = newFrame.na.drop()
      val joinCond = newFrame.columns.map(x=>frame.col(x) <=> newFrame.col(x)).reduce(_ && _)
      var resDf = frame.join(newDropFrame, joinCond)
      for(colName <- newFrame.columns){
        resDf = resDf.drop(newFrame.col(s"$colName"))
      }
      resDf.show()
      println("Ignore True case: Filter Not Null Values")
    }

    def nullCheck(frame: DataFrame, newFrame: DataFrame): Unit = {
      val filterCond = newFrame.columns.map(x=>col(x).isNull).reduce(_ || _)
      val filteredDf = newFrame.withColumn("result", when(filterCond, true).otherwise(false))
      val joinCond = newFrame.columns.map(x=>frame.col(x) <=> newFrame.col(x)).reduce(_ && _)
      var resDf = frame.join(filteredDf, joinCond)
      for(colName <- newFrame.columns){
        resDf = resDf.drop(newFrame.col(s"$colName"))
      }
      resDf.show()
      println("Ignore False Null check case: True for null values in list")
    }

    def notNullCheck(frame: DataFrame, newFrame: DataFrame): Unit = {
      val filterCond = newFrame.columns.map(x=>col(x).isNotNull).reduce(_ && _)
      val filteredDf = newFrame.withColumn("result", when(filterCond, true).otherwise(false))
      val joinCond = newFrame.columns.map(x=>frame.col(x) <=> newFrame.col(x)).reduce(_ && _)
      var resDf = frame.join(filteredDf, joinCond)
      for(colName <- newFrame.columns){
        resDf = resDf.drop(newFrame.col(s"$colName"))
      }
      resDf.show()
      println("Ignore False Not Null check case: True for not null values in list")
    }

    def emptyCheck(frame: DataFrame, newFrame: DataFrame): Unit = {
      val filterCond = newFrame.columns.map(x=>col(x) === "").reduce(_ || _)
      val filteredDf = newFrame.withColumn("result", when(filterCond, true).otherwise(false))
      val joinCond = newFrame.columns.map(x=>frame.col(x) <=> newFrame.col(x)).reduce(_ && _)
      var resDf = frame.join(filteredDf, joinCond)
      for(colName <- newFrame.columns){
        resDf = resDf.drop(newFrame.col(s"$colName"))
      }
      resDf.show()
      println("Ignore False Empty check case: True for empty values in list")
    }

    Check(someDF, List("class", "word"), "null", true)
    Check(someDF, List("class", "word"), "null", false)
    Check(someDF, List("class", "word"), "notnull", false)
    Check(someDF, List("class", "word"), "empty", false)
    println("Bye from this App")
  }
}

