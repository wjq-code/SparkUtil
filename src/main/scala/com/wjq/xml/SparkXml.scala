package com.wjq.xml

import com.wjq.csv.CsvUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * spark 读取xml文件以及把读取的内容保存为xml
 */
object SparkXml {
  /**
   * 通过spark获取Xml文件，并转换为DataFrame
   *
   * @param sparkSession
   * @param path
   */
  def getXmlData(
      sparkSession: SparkSession,
      path: String): DataFrame = {
    sparkSession.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "activation")
      .load(path)
  }

  def writeXml()={

  }


  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("SparkXmlTest")
      .master("local[*]")
      .getOrCreate()
    val path = "file:///D:\\idea_workspace\\SparkUtil\\File\\test.xml"
    val df = getXmlData(sparkSession,path)
    df.printSchema()
    df.show(false)
//    CsvUtil.saveCsv(df,"file:///D:\\idea_workspace\\SparkUtil\\File\\xml2csv.csv")
    CsvUtil.getCsvData(sparkSession,"file:///D:\\idea_workspace\\SparkUtil\\File\\xml2csv.csv\\part-00000-9e47537f-cdce-47be-94ab-8eb9ca43f6a3-c000.csv").show(false)
  }
}
