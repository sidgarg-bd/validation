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
      Row(8, "bat", 1),
      Row(null, "cat", 2),
      Row(-27, "horse", null)
    )

    val someSchema = List(
      StructField("number", IntegerType, true),
      StructField("word", StringType, true),
      StructField("id", IntegerType, true)
    )

    val someDF = spark.createDataFrame(
      sc.parallelize(someData),
      StructType(someSchema)
    )

    def Check(df: DataFrame, ls: List[String], condition: String, sze: Int = 4, ignore: Boolean): Unit =
    {
      val newDf = df.select(ls.map(col): _*)
      if(ignore == true)
      {
        val res = ignoreTrue(df: DataFrame, newDf: DataFrame, ls: List[String],condition: String, sze: Int)
      }
      else
      {
        if (condition == "null")
        {
          val res = nullCheck(df: DataFrame, newDf: DataFrame, ls: List[String])
        }
        else if(condition == "notnull")
        {
          val res = notNullCheck(df: DataFrame, newDf: DataFrame, ls: List[String])
        }
        else if (condition == "empty")
        {
          val res = emptyCheck(newDf: DataFrame)
        }
        else if (condition == "length")
        {
          if (ls.length == 1)
          {
            val res = lengthCheck(df: DataFrame, newDf: DataFrame, ls: List[String], sze: Int)
          }
          else
          {
            println("Column list for length check must be 1..EXITING")
            System.exit(1)
          }
        }
      }
    }

    def ignoreTrue(frame: DataFrame, newFrame: DataFrame, lst: List[String],cond: String, sz: Int): Unit = {
      if(cond == "length"){
        if (lst.length == 1)
        {
          for (colName <- lst) {
            val newLenFrame = newFrame.filter(length(col(colName)) === sz)
            println("Ignore True case: Filter Values with Length")
            val resDf = frame.join(newLenFrame, lst).show()
          }
        }
        else
        {
          println("Column list for length check must be 1..EXITING")
          System.exit(1)
        }
      }
      else{
        val newDropFrame = newFrame.na.drop()
        println("Ignore True case: Filter Not Null Values")
        val resDf = frame.join(newDropFrame, lst).show()
      }
    }

    def nullCheck(frame: DataFrame, newFrame: DataFrame, lst: List[String]): Unit = {
      val filterCond = newFrame.columns.map(x=>col(x).isNull).reduce(_ || _)
      val filteredDf = newFrame.withColumn("result", when(filterCond, true).otherwise(false))
      val resDf = frame.join(filteredDf, lst).show()
    }

    def notNullCheck(frame: DataFrame, newFrame: DataFrame, lst: List[String]): Unit = {
      val filterCond = newFrame.columns.map(x=>col(x).isNotNull).reduce(_ && _)
      val filteredDf = newFrame.withColumn("result", when(filterCond, true).otherwise(false))
      val resDf = frame.join(filteredDf, lst).show()
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

    def lengthCheck(frame: DataFrame, newFrame: DataFrame, lst: List[String], sz: Int): Unit = {
      for (colName <- lst) {
        val filteredDf = newFrame.withColumn("result",when(length(col(colName)) === sz, true).otherwise(false))
        val resDf = frame.join(filteredDf, lst).show()
      }
    }


    Check(someDF, List("word"), "length", 3, true)
    Check(someDF, List("number", "word"), "null", 1, false)
    Check(someDF, List("number", "word"), "notnull", 1, false)
    Check(someDF, List("number", "word"), "empty", 1, false)
    Check(someDF, List("word"), "length", 3, false)
    Check(someDF, List("number", "word"), "length", 3, true)
    println("Bye from this App")
  }
}

