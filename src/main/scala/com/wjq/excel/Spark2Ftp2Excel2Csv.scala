package com.wjq.excel

import java.io.File

import com.wjq.csv.CsvUtil
import com.wjq.ftp.SparkFtp
import com.wjq.hive.outputFormat.MyOutPutFormat
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
 * 用spark读取Ftp中的Excel文件，最后把Excel文件转换为Csv文件，存储到HDFS中
 * 1、连接FTP
 * 2、读取FTP中的excel文件（xls、xlsx）
 * 3、把Excel文件转换为Csv文件，保存文件到HDFS中
 */
object Spark2Ftp2Excel2Csv {
  def main(args: Array[String]): Unit = {
    val url = "192.168.1.223"
    val username = "test"
    val password = "test"
    val path = "/"
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val ftpClient = SparkFtp.getFtp(url, username, password, path)

    val listFile = ftpClient.listFiles()
    for (file <- listFile) {
      var fileName: String = null
      var suffix: String = null
      if (file.isFile) {
        fileName = file.getName
        // 获取文件的后缀名
        suffix = fileName.substring(fileName.lastIndexOf("."))
        if (suffix == ".xls" || suffix == ".xlsx") {
          val file = new File(s"D:\\idea_workspace\\SparkUtil\\File\\$fileName")
          val is = ftpClient.retrieveFileStream(fileName)

          FileUtils.copyInputStreamToFile(is, file)
          // 用流的时候一定要加
          ftpClient.completePendingCommand()
          val df = sparkSession.read
            .format("com.crealytics.spark.excel")
            .option("useHeader", "true") // 是否将第一行作为表头
            .option("inferSchema", "false") // 是否推断schema
            .option("workbookPassword", "None") // excel文件的打开密码
            .load(s"file:///D:\\idea_workspace\\SparkUtil\\File\\$fileName")

          val rdd = df.rdd
          // 获取文件中的记录条数
          println(s"${fileName}文件中的记录条数为" + rdd.count())

          CsvUtil.saveCsv(df, s"/xls/20191105$fileName")
        }
      }
    }
  }

}
