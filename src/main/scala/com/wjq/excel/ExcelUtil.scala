package com.wjq.excel

import com.wjq.csv.CsvUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object ExcelUtil {

  val peopleSchema = StructType(Array(
    StructField("id", IntegerType, nullable = true),
    StructField("name", StringType, nullable = true),
    StructField("age", StringType, nullable = true),
    StructField("sex", StringType, nullable = true)
  ))

  def getExcelUtil(sparkSession: SparkSession) = {
    val df = sparkSession.read
      .format("com.crealytics.spark.excel")
      .option("sheetName", "Sheet1")
      .option("useHeader", "true")
      .schema(peopleSchema)
      .load("ftp://test:test@192.168.1.223:21/test.xls")


    df.show()
    /*第二种方法，不指定schema,自动判断*/
//      .format("com.crealytics.spark.excel")
//      .option("useHeader", "true") // 是否将第一行作为表头
//      .option("inferSchema", "false") // 是否推断schema
//      .option("workbookPassword", "None") // excel文件的打开密码
//      .load("/xlsx/test.xlsx") //excel文件路径

//    CsvUtil.saveCsv(df,"hdfs://hadoop:9000/csv/20191104")
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    getExcelUtil(sparkSession)


  }
}
