package com.wjq.jdbc

import com.wjq.csv.CsvUtil
import com.wjq.job.SqlConf
import org.apache.spark.sql.{DataFrameReader, SparkSession}

object MySqlUtil {
  /**
   * spark读取mysql中的数据，并把每张表中的数据写入到hdsf中
   * @param sparkSession
   * @param sqlConf
   */
  def getMysqlDate(sparkSession: SparkSession, sqlConf: SqlConf) = {
    val dataFrameR: DataFrameReader = sparkSession.read
//      .format("jdbc")
//      .option("url", sqlConf.url)
//      .option("user", sqlConf.username)
//      .option("password", sqlConf.password)
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("user", "root")
      .option("password", "root")


    val list = sqlConf.getTable
    for (elem <- list) {
      println(elem)
      val dataFrame = dataFrameR
        .option("dbtable", elem)
        .load()

      // 把数据写入到csv中
      CsvUtil.saveCsv(dataFrame, "hdfs://hadoop:9000/csv/20131104")
    }
  }
}
