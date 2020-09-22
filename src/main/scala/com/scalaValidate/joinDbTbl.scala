package com.scalaValidate

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}

object joinDbTbl {
  def main(args: Array[String]) {
    println("Hello from App")
    val conf = new SparkConf()
      .setAppName("POC")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    implicit def getConfig(): Config = {
      ConfigFactory.parseFile(new File("src/main/resources/appl.conf"))
    }

    def joinTbl(): DataFrame = {
      val config: Config = getConfig()
      val url = config.getString("mysql.url")
      val driver = config.getString("mysql.driver")
      val username = config.getString("mysql.username")
      val password = config.getString("mysql.password")

      val jdbcDF1 = spark.read
        .format("jdbc")
        .option("url", url)
        .option("driver", driver)
        .option("dbtable", "emp")
        .option("user", username)
        .option("password", password)
        .load()

      val jdbcDF2 = spark.read
        .format("jdbc")
        .option("url", url)
        .option("driver", driver)
        .option("dbtable", "empdata")
        .option("user", username)
        .option("password", password)
        .load()

      val jdbcDF3 = spark.read
        .format("jdbc")
        .option("url", url)
        .option("driver", driver)
        .option("dbtable", "empsal")
        .option("user", username)
        .option("password", password)
        .load()

      val jdbcDF4 = spark.read
        .format("jdbc")
        .option("url", url)
        .option("driver", driver)
        .option("dbtable", "empdept")
        .option("user", username)
        .option("password", password)
        .load()

      val jdbcDF5 = spark.read
        .format("jdbc")
        .option("url", url)
        .option("driver", driver)
        .option("dbtable", "dept")
        .option("user", username)
        .option("password", password)
        .load()

      val joinedDf = jdbcDF1.join(jdbcDF2, Seq("empId"), "inner")
        .join(jdbcDF3, Seq("empId"), "inner")
        .join(jdbcDF4, Seq("empId"), "inner")
        .join(jdbcDF5, Seq("deptId"), "inner")

      return joinedDf
    }
    joinTbl().show()
  }
}



