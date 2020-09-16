package com.scalaValidate

import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.DataFrame

object disValidate {
  def disCheck(df: DataFrame, ignore: Boolean): DataFrame = {
    var resultantDF: DataFrame = null
    if (ignore == true) {
      resultantDF = ignoreTrue(df: DataFrame)
      return resultantDF
    }
    else {
      resultantDF = ignoreFalse(df: DataFrame)
      return resultantDF
    }
  }

  def ignoreTrue(frame: DataFrame): DataFrame = {
    val resDf = frame.dropDuplicates()
    println("Ignore True case: Filter Duplicates")
    return resDf
  }

  def ignoreFalse(frame: DataFrame): DataFrame = {
    val cond: DataFrame = frame.groupBy(frame.columns.map(col):_*).count()
    val filteredDf = cond.withColumn("result", when(col("count") > 1, true).otherwise(false)).drop("count")
    val joinCond = frame.columns.map(x=>frame.col(x) <=> filteredDf.col(x)).reduce(_ && _)
    var resDf = frame.join(filteredDf, joinCond)
    for(colName <- frame.columns){
      resDf = resDf.drop(frame.col(s"$colName"))
    }
    println("Ignore False Distinct check case: True for Duplicates")
    return resDf
  }
}