package com.scalaValidate

import com.scalaValidate.validate.Check
import com.scalaValidate.compValidate.compCheck
import com.scalaValidate.disValidate.disCheck
import com.scalaValidate.lengthValidate.lengthCheck
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

object App {
  def main(args: Array[String]) {
    try {
      println("Hello from App")
      val conf = new SparkConf()
        .setAppName("POC")
        .setMaster("local[2]")

      val sc = new SparkContext(conf)
      val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

      val someData = Seq(
        Row("1", "1", "AUS"),
        Row("7", "4", ""),
        Row("3", null, "Mass"),
        Row("4", "3", ""),
        Row("6", "3", "Minnesota"),
        Row("", "4", "Dart"),
        Row("", "4", "Dart"),
        Row("7", "6", ""),
        Row("1", "0", "AUS"),
        Row("4", "4", "AS"),
        Row("1", "0", "AUS"),
        Row("8", "9", ""),
        Row("1", "0", "AUS"),
        Row("4", "4", "AUS")
      )

      val someSchema = List(
        StructField("id", StringType, true),
        StructField("class", StringType, true),
        StructField("word", StringType, true)
      )

//      val someDF = spark.createDataFrame(
//        sc.parallelize(someData),
//        StructType(someSchema)
//      )

      val someDF = spark.read.format("csv")
        .option("header", "true")
        .load("src/main/resources/test.csv")

      val df1 = Check(someDF, List("class", "word"), "null", true)
      val df2 = Check(someDF, List("class", "word"), "null", false)
      val df3 = Check(someDF, List("class", "word"), "notnull", false)
      val df4 = Check(someDF, List("class", "word"), "empty", false)
      val df5 = Check(someDF, List("class", "word"), "empty", true)

      val df11 = compCheck(someDF, List("class", "word"), "===", 3, true)
      val df12 = compCheck(someDF, List("class", "word"), ">", 3, true)
      val df13 = compCheck(someDF, List("class", "word"), "<", 3, true)
      val df14 = compCheck(someDF, List("class", "word"), "<=", 3, true)
      val df15 = compCheck(someDF, List("class", "word"), ">=", 3, true)
      val df16 = compCheck(someDF, List("class", "word"), "===", 3, false)
      val df17 = compCheck(someDF, List("class", "word"), ">", 3, false)
      val df18 = compCheck(someDF, List("class", "word"), "<", 3, false)
      val df19 = compCheck(someDF, List("class", "word"), "<=", 3, false)
      val df20 = compCheck(someDF, List("class", "word"), ">=", 3, false)
//      val df200 = compCheck(someDF, List("class", "word"), "====", 3, true)

      val df21 = lengthCheck(someDF, "word", 3, true)
      val df22 = lengthCheck(someDF, "word", 3, false)

      val df31 = disCheck(someDF, true)
      val df32 = disCheck(someDF, false)

      println("Bye from this App")
    }

    catch{
      case x: AnalysisException => println(s"Analysis Exception: $x")
      case x: NullPointerException => println(s"Null Pointer Exception: $x")
      case unknown: Exception => println(s"Unknown Exception: $unknown")
    }
  }
}