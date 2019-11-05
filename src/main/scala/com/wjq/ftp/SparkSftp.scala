package com.wjq.ftp

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 类名称: SparkFtp
 * 类描述:
 *
 * @Time 2019/10/18 16:30
 * @author 武建谦
 */
object SparkSftp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val url = "192.168.1.223"
    val username = "test"
    val password = "test"
    val host = "192.168.95.1"
    val port = 21
    val path = "/"

    val ftp = getSFtp(sparkSession, url, username, password, path)
  }

  /**
   * 连接sftp
   */
  def getSFtp(sparkSession: SparkSession, url: String, userName: String, password: String, path: String) = {
    val df = sparkSession.read.
      format("com.springml.spark.sftp")
      .option("host", "192.168.1.223")
      .option("user", "test")
      .option("password", "test")
      .option("fileType", "txt")
      .option("connectTimeout", 0)
      .load("/test.txt")
    df.show()
  }
}
