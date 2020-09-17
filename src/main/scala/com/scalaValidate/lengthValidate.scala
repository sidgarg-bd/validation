package com.scalaValidate

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object lengthValidate {
  def lengthCheck(df: DataFrame, colName: String, sze: Int, ignore: Boolean): DataFrame =
  {
    var resultantDF:DataFrame = null
    if (ignore == true) {
      resultantDF= ignoreTrue(df: DataFrame, colName: String, sze: Int)
      println(s"Ignore True Length check case: Filter values having Length $sze values in column $colName")
      return resultantDF
    }
    else {
      resultantDF = ignoreFalse(df: DataFrame, colName: String, sze: Int)
      println(s"Ignore False Length check case: True for Length $sze values in column $colName")

      return resultantDF
    }
  }

  def ignoreTrue(frame: DataFrame, colNam: String, sz: Int): DataFrame = {
    val finalDf = frame.filter(length(col(colNam)) === sz)
    return finalDf
  }

  def ignoreFalse(frame: DataFrame, colNam: String, sz: Int): DataFrame = {
    val finalDf = frame.withColumn("result", when(length(col(colNam)) === sz, true).otherwise(false))
    return finalDf
  }
}

