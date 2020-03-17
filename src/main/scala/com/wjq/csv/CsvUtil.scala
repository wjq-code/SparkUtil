package com.wjq.csv

import com.wjq.job.{SqlConf, TestTable}
import org.apache.flink.table.api.scala.row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvUtil {
  def getCsvData(sparkSession: SparkSession, path: String) = {
    sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("multiLine", "true")
      .option("inferSchema", "true")
      .option("delimiter","|")
      .option("escape","+!")
      .option("codec","bzip2")
      .load(path)
  }

  def saveCsv(dataFrame: DataFrame, path: String): Unit = {
    dataFrame.write
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
//      .option("delimiter","|")
      .option("escape","!")
      .option("multiLine",true)
//      .option("quoteMode","ALL")
//      .option("codec","bzip2")
//      .save(path)
      .save(path)
  }

  /**
   * 读取csv文件
   * 根据schema来读取csv文件
   * @param sparkSession
   * @return
   */
  def readCsv(sparkSession: SparkSession, path: String) = {
    val df = sparkSession.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "false")
      .schema(ScalaReflection.schemaFor[TestTable].dataType.asInstanceOf[StructType])
      .load(path)
  }
}
