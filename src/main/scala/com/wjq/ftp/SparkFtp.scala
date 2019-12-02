package com.wjq.ftp

import org.apache.commons.net.ftp.{FTP, FTPClient, FTPClientConfig, FTPReply}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * 类名称: SparkFtp
  * 类描述: 
  *
  * @Time 2019/10/18 16:30
  * @author 武建谦
  */
object SparkFtp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val url = "192.168.1.223"
    val username = "test"
    val password = "test"
    val host = "192.168.95.1"
    val port = 21
    val path = "/"

    val ftp = getFtp(url, username, password, path)

        val files = ftp.listFiles()
        for(file <- files){
          println(file.getName)
        }


  }

  /**
    * 连接ftp
    *
    * @param url
    * @param userName
    * @param password
    * @param path
    * @return
    */
  def getFtp(url: String, userName: String, password: String, path: String): FTPClient = {
    // 指定ftp安装位置的类型（windows  unix）慎重
    val conf = new FTPClientConfig(FTPClientConfig.SYST_NT)
    val ftp = new FTPClient
    ftp.configure(conf)
    // 调节字符编码问题
    ftp.setControlEncoding("GBK")
    // 创建连接
    ftp.connect(url, 21)
    // 登录
    ftp.login(userName, password)

    //设置超时时间
    ftp.setDataTimeout(120000)
//    FTPReply.isPositiveCompletion()

    // 设置文件类型为二进制
    ftp.setFileType(FTP.BINARY_FILE_TYPE)
    // 被动模式
    ftp.enterLocalPassiveMode()
    // 改变工作目录
    ftp.changeWorkingDirectory(path)
    ftp
  }
}
