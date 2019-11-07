package com.wjq.ftp

import java.io.File

import com.wjq.csv.CsvUtil
import com.wjq.excel.ExcelUtil
import org.apache.commons.net.ftp.FTPClient
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object FTP2HDFS {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val url = "192.168.1.223"
    val username = "test"
    val password = "test"
    val path = "/"

    val ftpClient = SparkFtp.getFtp(url, username, password, path)
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://hadoop:9000");
    val fs = FileSystem.get(conf)
    ftp2Hdsf(ftpClient, fs, conf, sparkSession)
  }

  def ftp2Hdsf(ftp: FTPClient, fs: FileSystem, conf: Configuration, sparkSession: SparkSession) {
    val listFile = ftp.listFiles()
    for (file <- listFile) {
      var fileName: String = null
      var suffix: String = null
      if (file.isFile) {
        fileName = file.getName
        // 获取文件的后缀名
        suffix = fileName.substring(fileName.lastIndexOf("."))
        if (suffix == ".xls" || suffix == ".xlsx") {
          println(fileName)
          val is = ftp.retrieveFileStream(fileName)
          val patha = "/ftp2Hdfs_test/" + fileName
          val path = new Path(patha)
          if (fs.exists(path)) {
            fs.delete(path, true)
          }
          val outputStream = fs.create(path)
          IOUtils.copyBytes(is, outputStream, conf, true)
          is.close()
          ftp.completePendingCommand()

          ExcelUtil.getExcelUtil(sparkSession, patha)
        }
      }
    }
  }
}