package com.scalaValidate

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, length, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object compValidate {
  def main(args: Array[String]): Unit = {
    try{
      println("Hello from App")
      val conf = new SparkConf()
        .setAppName("POC")
        .setMaster("local[2]")

      val sc = new SparkContext(conf)
      val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

      val someDF = spark.read.format("csv")
        .option("header", "true")
        .load("src/main/resources/test.csv")

      def compCheck(df: DataFrame, ls: List[String], condition: String, compValue: Int, ignore: Boolean): Unit =
      {
        var newDf = df.select(ls.map(col): _*)
        newDf = newDf.columns.foldLeft(newDf)((current, c) => current.withColumn(c, col(c).cast("int")))
        val list = Seq(">=","<=","<",">","===")
        if (list.contains(condition)){
          if (ignore == true) {
            val res = ignoreTrue(df: DataFrame, newDf: DataFrame, condition: String, compValue: Int)
          }
          else {
            val res = ignoreFalse(df: DataFrame, newDf: DataFrame, condition: String, compValue: Int)
          }
        }
        else{
          println(s"Your condition $condition is Invalid")
          println(s"Pass valid Condition Out of: >=,<=,<,>,===")
          System.exit(1)
        }
      }

      def ignoreTrue(frame: DataFrame, newFrame: DataFrame, cond: String, compVal: Int): Unit = {
        val filterCond = cond match {
          case ">=" => newFrame.columns.map(x=> col(x) >= compVal).reduce(_ && _)
          case "<=" => newFrame.columns.map(x=> col(x) <= compVal).reduce(_ && _)
          case ">" => newFrame.columns.map(x=> col(x) > compVal).reduce(_ && _)
          case "<" => newFrame.columns.map(x=> col(x) < compVal).reduce(_ && _)
          case "===" => newFrame.columns.map(x=> col(x) === compVal).reduce(_ && _)
        }
        val filteredDf = newFrame.filter(filterCond)
        val joinCond = newFrame.columns.map(x=>frame.col(x) <=> newFrame.col(x)).reduce(_ && _)
        var resDf = frame.join(filteredDf, joinCond)
        for(colName <- newFrame.columns){
          resDf = resDf.drop(newFrame.col(s"$colName"))
        }
        resDf.dropDuplicates().show()
        println(s"Ignore True Comparison check case: Filter values for $cond")
      }

      def ignoreFalse(frame: DataFrame, newFrame: DataFrame, cond: String, compVal: Int): Unit = {
        val filterCond = cond match {
          case ">=" => newFrame.columns.map(x=> col(x) >= compVal).reduce(_ && _)
          case "<=" => newFrame.columns.map(x=> col(x) <= compVal).reduce(_ && _)
          case ">" => newFrame.columns.map(x=> col(x) > compVal).reduce(_ && _)
          case "<" => newFrame.columns.map(x=> col(x) < compVal).reduce(_ && _)
          case "===" => newFrame.columns.map(x=> col(x) === compVal).reduce(_ && _)
        }
        val filteredDf = newFrame.withColumn("result", when(filterCond, true).otherwise(false))
        val joinCond = newFrame.columns.map(x=>frame.col(x) <=> newFrame.col(x)).reduce(_ && _)
        var resDf = frame.join(filteredDf, joinCond)
        for(colName <- newFrame.columns){
          resDf = resDf.drop(newFrame.col(s"$colName"))
        }
        resDf.dropDuplicates().show()
        println(s"Ignore False Comparison check case: True for values for $cond")
      }

      compCheck(someDF, List("class", "word"), "===", 3, true)
      compCheck(someDF, List("class", "word"), ">", 3, true)
      compCheck(someDF, List("class", "word"), "<", 3, true)
      compCheck(someDF, List("class", "word"), "<=", 3, true)
      compCheck(someDF, List("class", "word"), ">=", 3, true)
      compCheck(someDF, List("class", "word"), "===", 3, false)
      compCheck(someDF, List("class", "word"), ">", 3, false)
      compCheck(someDF, List("class", "word"), "<", 3, false)
      compCheck(someDF, List("class", "word"), "<=", 3, false)
      compCheck(someDF, List("class", "word"), ">=", 3, false)
      compCheck(someDF, List("class", "word"), "====", 3, true)
      println("Bye from this App")
    }

    catch{
      case x: AnalysisException => println(s"Analysis Exception: $x")
      case x: NullPointerException => println(s"Null Pointer Exception: $x")
      case unknown: Exception => println(s"Unknown Exception: $unknown")
    }
  }
}
