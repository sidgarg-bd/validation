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
      Row(1, 8, "bat"),
      Row(2, null, "cat"),
      Row(null, -27, "horse")
    )

    val someSchema = List(
      StructField("id", IntegerType, true),
      StructField("number", IntegerType, true),
      StructField("word", StringType, true)
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
            val joinCond = newFrame.columns.map(x=>frame.col(x) <=> newFrame.col(x)).reduce(_ && _)
            var resDf = frame.join(newLenFrame, joinCond)
            for(colName <- newFrame.columns){
              resDf = resDf.drop(newFrame.col(s"$colName"))
            }
            resDf.show()
            println("Ignore True case: Filter Values with Length")
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
        val joinCond = newFrame.columns.map(x=>frame.col(x) <=> newFrame.col(x)).reduce(_ && _)
        var resDf = frame.join(newDropFrame, joinCond)
        for(colName <- newFrame.columns){
          resDf = resDf.drop(newFrame.col(s"$colName"))
        }
        resDf.show()
        println("Ignore True case: Filter Not Null Values")
      }
    }

    def nullCheck(frame: DataFrame, newFrame: DataFrame, lst: List[String]): Unit = {
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

    def notNullCheck(frame: DataFrame, newFrame: DataFrame, lst: List[String]): Unit = {
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
        val joinCond = newFrame.columns.map(x=>frame.col(x) <=> newFrame.col(x)).reduce(_ && _)
        var resDf = frame.join(filteredDf, joinCond)
        for(colName <- newFrame.columns){
          resDf = resDf.drop(newFrame.col(s"$colName"))
        }
        resDf.show()
        println("Ignore False Length check case: True for correct length values in list")
      }
    }


    Check(someDF, List("word"), "length", 3, true)
    Check(someDF, List("number", "word"), "null", 1, true)
    Check(someDF, List("number", "word"), "null", 1, false)
    Check(someDF, List("number", "word"), "notnull", 1, false)
    Check(someDF, List("number", "word"), "empty", 1, false)
    Check(someDF, List("word"), "length", 3, false)
    Check(someDF, List("number", "word"), "length", 3, true)
    println("Bye from this App")
  }
}

