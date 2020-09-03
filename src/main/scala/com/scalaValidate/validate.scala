package com.scalaValidate

import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

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

    def Check(df: DataFrame, ls: List[String], condition: String, ignore: Boolean): Unit = {
      val newDf = df.select(ls.map(col): _*)
      if (ignore == true) {
        val res = ignoreTrue(newDf: DataFrame)
      }
      else {
        if (condition == "null") {
          val res = nullCheck(newDf: DataFrame, ls: List[String])
          //println(res)
        }
      }

    }

    def ignoreTrue(frame: DataFrame): Unit = {
      frame.na.drop().show()
      //      var frame1: DataFrame = spark.createDataFrame(sc.emptyRDD[Row], frame.schema)
      //      val len = colList.length
      //      for (i <- 0 until len) {
      //        frame1 = frame.filter(col(colList[i] && col).isNotNull).union(frame1)
      //      }
      //      frame1.show()
    }

    def nullCheck(frame: DataFrame, ls: List[String]): Unit = {
      val filterCond = frame.columns.map(x => col(x).isNull).reduce(_ || _)
      print(filterCond)
      val filteredDf = frame.withColumn("result", when(filterCond, true).otherwise(false)).show()
      //      cond ='|'.join('(col("'+str(_)+'")==0)' for _ in range(1, 12))
      //
      //      cond = '('+cond+')'
      //
      //      print(cond)
      //      val cond = for(l <- ls){println(("col('"+l+"').isNull").mkString("|"))}
      //      println(cond)
      //{(frame.col(colu).isNull)}
      //println(con)
      //      cond_expr = functools.reduce(operator.or_, [(f.col(c) == 0) for c in df1.columns])
      //      frame.withColumn("NewCol", when(col("A").isNull,true).when(col("B").isNull,0).when(col("C") === 0,0).otherwise(1))
      //      val coder = (df: DataFrame) => {for(colu <- df.columns){when(col(colu).isNull , true).otherwise(false)}}

      //      val sqlFun = udf(coder)
      //      frame.withColumn("result", sqlFun(frame.columns)).show()
      //      var check=0
      //      var res=false
      //      var frame1: DataFrame = spark.createDataFrame(sc.emptyRDD[Row], frame.schema)
      //      frame1 = frame1.withColumn("result", lit(true))
      //      for(colName <- lst){
      //        frame1 = frame.withColumn("result", when(col(colName).isNull, true).otherwise(false)).unionAll(frame1)
      //      }
      //      frame1.show()
      //      {
      //        if(frame.filter(col(colName).isNull).count() > 0){
      //          check=check+1
      //        }
      //      }
      //      println(check)
      //      if(check > 0){
      //        res = true
      //      }
      //      return res
    }
    Check(someDF, List("number", "word"), "null", true)
    Check(someDF, List("number", "word"), "null", false)
    //someDF.filter(col("word").isNull).count()
    println("Bye from this App")
  }
}
