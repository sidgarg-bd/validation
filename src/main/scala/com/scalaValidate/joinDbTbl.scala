package com.scalaValidate

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object joinDbTbl {

  def joinTbl(spark: SparkSession, tblList: List[String], colList: List[String], typeList: List[String], filPath: String): DataFrame =
  {
    val config: Config = ConfigFactory.parseFile(new File(filPath))
    val url = config.getString("mysql.url")
    val driver = config.getString("mysql.driver")
    val username = config.getString("mysql.username")
    val password = config.getString("mysql.password")

    var jdbctblDF = new ListBuffer[DataFrame]()
    for(tbl <- tblList){
      val jdbcDF = spark.read
        .format("jdbc")
        .option("url", url)
        .option("driver", driver)
        .option("dbtable", tbl)
        .option("user", username)
        .option("password", password)
        .load()

      jdbctblDF += jdbcDF
    }

    var joinedDf = jdbctblDF(0).join(jdbctblDF(1), Seq(colList(0)), typeList(0))
    for (i <- 2 to jdbctblDF.length-1) {
      joinedDf = joinedDf.join(jdbctblDF(i), Seq(colList(i-1)), typeList(i-1))
    }

    return joinedDf
  }
}
