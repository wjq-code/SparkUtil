package com.wjq.jdbc

import com.wjq.csv.CsvUtil
import com.wjq.job.SqlConf
import org.apache.hadoop.yarn.client.api.YarnClientApplication
import org.apache.hadoop.yarn.server.utils.YarnServerBuilderUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{DataFrameReader, Row, SparkSession}

import scala.collection.mutable.ListBuffer

object MySqlUtil2 {
  /**
   * spark读取mysql中的数据，并把每张表中的数据写入到hdsf中
   *
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
      .option("url", "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull")
      .option("user", "root")
      .option("password", "root")
    //      .option("dbtable", "test")
    //      .option("dbtable", "(select count(1) from test) t")
    //
    //    dataFrameR.load().show(false)
    var list1 = new ListBuffer[String]
    list1.+=("dwb_dm_lgxx_202001161013")
    sqlConf.setTable(list1)
    val list = sqlConf.getTable
    val dataFrame = dataFrameR
//      .option("dbtable", "(select * from dwb_dm_lgxx_202001161013 where lat is NULL and lng is NULL) t")
      .option("dbtable", "(SELECT * FROM `rj_dic_jz_34gjzxx` where domestic is null) t")
//      .option("dbtable", "(SELECT * FROM `pcs`where lat is NULL and lng is NULL) t")
      .load()
//    for (elem <- list) {
//      println(elem)
//      val dataFrame = dataFrameR
//        .option("dbtable", elem)
//        .load()
//      //      val schema = dataFrame.schema.add("id",LongType)
//      //
//      //      val dfRDD = dataFrame.rdd.zipWithIndex()
//      //      println("----------------------------------------------")
//      //      val rowRDD: RDD[Row] = dfRDD.map(tp => Row.merge(tp._1, Row(tp._2)))
//      //      val df2 = sparkSession.createDataFrame(rowRDD, schema)
//      //
//      ////      dataFrame.printSchema()
//      //      df2.show(false)
//      // 把数据写入到csv中
////      CsvUtil.saveCsv(dataFrame, "file:///E:\\jobrs\\csv")
////      CsvUtil.saveCsv(dataFrame, "/csv/testCsv")
//    }
    dataFrame
  }
}
