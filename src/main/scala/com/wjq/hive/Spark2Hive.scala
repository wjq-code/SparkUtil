package com.wjq.hive

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark2Hive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark2Hive").setMaster("local[2]")
    val sparkSession = SparkSession.builder()
      .config(conf)
      .config("spark.sql.warehouse.dir","spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()
    sparkSession.sql("")
  }
}
