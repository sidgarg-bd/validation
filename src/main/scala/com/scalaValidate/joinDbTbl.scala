package com.scalaValidate

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object joinDbTbl {

  def joinTbl(df: DataFrame,dbList: List[String], tblList: List[String], filCondList: List[List[String]], joinColList: List[List[String]], typeList: List[String], filPath: String)(implicit spark: SparkSession, sc: SparkContext) : DataFrame =
  {
    val config: Config = ConfigFactory.parseFile(new File(filPath))
    var jdbcDF: DataFrame = null
    var jdbctblDF = new ListBuffer[DataFrame]()
    for(((tbl, dbType), filCond) <- ((tblList zip dbList) zip filCondList)){
      if(dbType == "mysql"){
        val url = config.getString("mysql.url")
        val driver = config.getString("mysql.driver")
        val username = config.getString("mysql.username")
        val password = config.getString("mysql.password")

        jdbcDF = spark.read
          .format("jdbc")
          .option("url", url)
          .option("driver", driver)
          .option("dbtable", tbl)
          .option("user", username)
          .option("password", password)
          .load()
          .filter(date_format(col(filCond(0)), "yyyy-mm-dd").between(filCond(1), filCond(2)))
      }
      else if(dbType == "db2"){
        val url = config.getString("db2.url")
        val driver = config.getString("db2.driver")
        val username = config.getString("db2.username")
        val password = config.getString("db2.password")

        jdbcDF = spark.read
          .format("jdbc")
          .option("url", url)
          .option("driver", driver)
          .option("dbtable", tbl)
          .option("user", username)
          .option("password", password)
          .load()
          .filter(date_format(col(filCond(0)), "yyyy-mm-dd").between(filCond(1), filCond(2)))
      }
      else if(dbType == "hive"){
        val hc = new HiveContext(sc)
        jdbcDF = hc.sql(s"""select * from $tbl""")
          .filter(date_format(col(filCond(0)), "yyyy-mm-dd").between(filCond(1), filCond(2)))
      }
      jdbctblDF += jdbcDF
    }

    var joinedDf = df.join(jdbctblDF(0), joinColList(0), typeList(0))
    for (i <- 1 to jdbctblDF.length-1) {
      joinedDf = joinedDf.join(jdbctblDF(i), joinColList(i), typeList(i))
    }
    return joinedDf
  }
}