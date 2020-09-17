package com.scalaValidate

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object validate {
  def Check(df: DataFrame, ls: List[String], condition: String, ignore: Boolean): DataFrame =
  {
    val newDf = df.select(ls.map(col): _*)
    var resultantDF:DataFrame = null
    if(ignore == true)
    {
      if (condition == "empty")
      {
        resultantDF = ignoreTrueEmpty(df: DataFrame, newDf: DataFrame)
        println(s"Ignore True Empty check case: Filter Empty values in column's $ls")
        return resultantDF
      }
      else {
        resultantDF = ignoreTrue(df: DataFrame, newDf: DataFrame)
        println(s"Ignore True $condition check case: Filter Not Null values in column's $ls")
        return resultantDF
      }

    }
    else {
      {
        if (condition == "null")
        {
          resultantDF = nullCheck(df: DataFrame, newDf: DataFrame)
          println(s"Ignore False Null check case: True for Null values in column's $ls")
          return resultantDF
        }
        else if(condition == "notnull")
        {
          resultantDF = notNullCheck(df: DataFrame, newDf: DataFrame)
          println(s"Ignore False Not Null check case: True for Not Null values in column's $ls")
          return resultantDF
        }
        else if (condition == "empty")
        {
          resultantDF = emptyCheck(df: DataFrame, newDf: DataFrame)
          println(s"Ignore False Empty check case: True for Empty values in column's $ls")
          return resultantDF
        }
      }
    }
    return resultantDF
  }

  def ignoreTrue(frame: DataFrame, newFrame: DataFrame): DataFrame = {
    val newDropFrame = newFrame.na.drop()
    val joinCond = newFrame.columns.map(x=>frame.col(x) <=> newFrame.col(x)).reduce(_ && _)
    var resDf = frame.join(newDropFrame, joinCond)
    for(colName <- newFrame.columns){
      resDf = resDf.drop(newFrame.col(s"$colName"))
    }
    val finalDf: DataFrame = resDf.dropDuplicates()
    return finalDf
  }

  def ignoreTrueEmpty(frame: DataFrame, newFrame: DataFrame): DataFrame = {
    val filterCond = newFrame.columns.map(x=>col(x) =!= "").reduce(_ && _)
    val filteredDf = newFrame.filter(filterCond)
    val joinCond = newFrame.columns.map(x=>frame.col(x) <=> newFrame.col(x)).reduce(_ && _)
    var resDf = frame.join(filteredDf, joinCond)
    for(colName <- newFrame.columns){
      resDf = resDf.drop(newFrame.col(s"$colName"))
    }
    val finalDf: DataFrame = resDf.dropDuplicates()
    return finalDf
  }

  def nullCheck(frame: DataFrame, newFrame: DataFrame): DataFrame = {
    val filterCond = newFrame.columns.map(x=>col(x).isNull).reduce(_ || _)
    val filteredDf = newFrame.withColumn("result", when(filterCond, true).otherwise(false))
    val joinCond = newFrame.columns.map(x=>frame.col(x) <=> newFrame.col(x)).reduce(_ && _)
    var resDf = frame.join(filteredDf, joinCond)
    for(colName <- newFrame.columns){
      resDf = resDf.drop(newFrame.col(s"$colName"))
    }
    val finalDf = resDf.dropDuplicates()
    return finalDf
  }

  def notNullCheck(frame: DataFrame, newFrame: DataFrame): DataFrame = {
    val filterCond = newFrame.columns.map(x=>col(x).isNotNull).reduce(_ && _)
    val filteredDf = newFrame.withColumn("result", when(filterCond, true).otherwise(false))
    val joinCond = newFrame.columns.map(x=>frame.col(x) <=> newFrame.col(x)).reduce(_ && _)
    var resDf = frame.join(filteredDf, joinCond)
    for(colName <- newFrame.columns){
      resDf = resDf.drop(newFrame.col(s"$colName"))
    }
    val finalDf = resDf.dropDuplicates()
    return finalDf
  }

  def emptyCheck(frame: DataFrame, newFrame: DataFrame): DataFrame = {
    val filterCond = newFrame.columns.map(x=>col(x) === "").reduce(_ || _)
    val filteredDf = newFrame.withColumn("result", when(filterCond, true).otherwise(false))
    val joinCond = newFrame.columns.map(x=>frame.col(x) <=> newFrame.col(x)).reduce(_ && _)
    var resDf = frame.join(filteredDf, joinCond)
    for(colName <- newFrame.columns){
      resDf = resDf.drop(newFrame.col(s"$colName"))
    }
    val finalDf = resDf.dropDuplicates()
    return finalDf
  }
}