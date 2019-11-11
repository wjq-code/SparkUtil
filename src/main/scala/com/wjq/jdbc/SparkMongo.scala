package com.wjq.jdbc

import com.mongodb.spark.MongoSpark
import com.wjq.csv.CsvUtil
import com.wjq.job.SqlConf
import com.wjq.schema.SchemaUtil
import org.apache.spark.sql.{DataFrameReader, SparkSession}

import scala.collection.mutable.ListBuffer

object SparkMongo {
  /**
   * spark读取mysql中的数据，并把每张表中的数据写入到hdsf中
   *
   * @param sparkSession
   * @param sqlConf
   */
  def getMongoDate(sparkSession: SparkSession, sqlConf: SqlConf) = {
    val url = sqlConf.url
    val username = sqlConf.username
    val password = sqlConf.password

    val dataFrameR = MongoSpark.read(sparkSession)
      .option("spark.mongodb.input.uri", "mongodb://" + url)
      .option("spark.mongodb.input.hint", "json")

    val list = sqlConf.getTable
    for (elem <- list) {
      println(elem)
      val dataFrame = dataFrameR
        .option("spark.mongodb.input.collection", elem)
        .load().drop("_id")
      dataFrame.show(false)
      //      // 把数据写入到csv中
            val mongoData = SchemaUtil.createDataFrame(sparkSession,dataFrame)
            CsvUtil.saveCsv(mongoData, "hdfs://hadoop:9000/mongo/20131104")
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("testMongo")
      .master("local[2]")
      .getOrCreate()
    val conf = new SqlConf()
    conf.setUrl("127.0.0.1:27017/test")
    conf.setUsername("root")
    conf.setPassword("root")
    conf.setDriver("")
    var list: ListBuffer[String] = new ListBuffer[String]
    list += ("user")
    conf.setTable(list)

    getMongoDate(sparkSession, conf)
  }
}
