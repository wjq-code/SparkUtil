package com.wjq.csv

import com.wjq.collection.GlobalConstant
import com.wjq.jdbc.MySqlUtil
import com.wjq.job.SqlConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object CsvTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    //    val sc = new SparkContext(conf)
    //    sc.setLogLevel("WARN")
    val sparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    //    val mysqlConf = new SqlConf()
    //    mysqlConf.setUrl("jdbc:mysql://localhost:3306/test")
    //    mysqlConf.setUrl("root")
    //    mysqlConf.setPassword("root")
    //    mysqlConf.setDriver(GlobalConstant.MYSQL_DRIVER)
    //    var list = new ListBuffer[String]
    //    list.+=("data")
    //    mysqlConf.setTable(list)
    //
    //    MySqlUtil.getMysqlDate(sparkSession,mysqlConf)

    val df = sparkSession.read.format("csv").load("file:///e:/FTP/tong/part-00000-1a807141-4083-46af-81da-9ef1752b4e69.csv")
    val a = df.count()
    println(a)

  }
}
