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
      .option("multiLine", true)
      .load(path)
  }

  def saveCsv(dataFrame: DataFrame, path: String): Unit = {
    dataFrame.write
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
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
