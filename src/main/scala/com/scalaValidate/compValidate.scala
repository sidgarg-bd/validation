package com.scalaValidate

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col,when}

object compValidate {
  def compCheck(df: DataFrame, ls: List[String], condition: String, compValue: Int, ignore: Boolean): DataFrame =
  {
    var newDf = df.select(ls.map(col): _*)
    newDf = newDf.columns.foldLeft(newDf)((current, c) => current.withColumn(c, col(c).cast("int")))
    val list = Seq(">=","<=","<",">","===")
    var resultantDF:DataFrame = null
    if (list.contains(condition)){
      if (ignore == true) {
        resultantDF = ignoreTrue(df: DataFrame, newDf: DataFrame, condition: String, compValue: Int)
        return resultantDF
      }
      else {
        resultantDF = ignoreFalse(df: DataFrame, newDf: DataFrame, condition: String, compValue: Int)
        return resultantDF
      }
    }
    else{
      println(s"Your condition $condition is Invalid")
      println(s"Pass valid Condition Out of: >=,<=,<,>,===")
      System.exit(1)
      return resultantDF
    }
  }

  def ignoreTrue(frame: DataFrame, newFrame: DataFrame, cond: String, compVal: Int): DataFrame = {
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
    println(s"Ignore True Comparison check case: Filter values for $cond")
    return resDf
  }

  def ignoreFalse(frame: DataFrame, newFrame: DataFrame, cond: String, compVal: Int): DataFrame = {
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
    println(s"Ignore False Comparison check case: True for values for $cond")
    return resDf
  }
}