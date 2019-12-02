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
      .option("url", "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull")
      .option("user", "root")
      .option("password", "root")
      .option("timeStampFormat", "yyyy/MM/dd HH:mm:ss")
    // 当面临百万级别的数据量时
    val url = "jdbc:mysql://mysqlHost:3306/database"
    val tableName = "table"
    // columnName这个字段必须是int类型
    val columnName = "colName"
    val lowerBound = 1
    val upperBound = 10000000
    val numPartitions = 10

    // 设置连接用户&密码
    val prop = new java.util.Properties
    prop.setProperty("user","username")
    prop.setProperty("password","pwd")

    // 取得该表数据
//    val jdbcDF = sqlContext.read.jdbc(url,tableName,columnName,lowerBound,upperBound,numPartitions,prop)


    // 不同的字段类型
//    val url = "jdbc:mysql://mysqlHost:3306/database"
//    val tableName = "table"

    // 设置连接用户&密码
//    val prop = new java.util.Properties
//    prop.setProperty("user","username")
//    prop.setProperty("password","pwd")

    /**
     * 将9月16-12月15三个月的数据取出，按时间分为6个partition
     * 为了减少事例代码，这里的时间都是写死的
     * modified_time 为时间字段
     */
    val predicates =
      Array(
        "2015-09-16" -> "2015-09-30",
        "2015-10-01" -> "2015-10-15",
        "2015-10-16" -> "2015-10-31",
        "2015-11-01" -> "2015-11-14",
        "2015-11-15" -> "2015-11-30",
        "2015-12-01" -> "2015-12-15"
      ).map {
        case (start, end) =>
          s"cast(modified_time as date) >= date '$start' " + s"AND cast(modified_time as date) <= date '$end'"
      }

    // 取得该表数据
//    val jdbcDF = sqlContext.read.jdbc(url,tableName,predicates,prop)


    val list = sqlConf.getTable
    for (elem <- list) {
      println(elem)
      val dataFrame = dataFrameR
        .option("dbtable", elem)
        .load()
      dataFrame.printSchema()
      dataFrame.show(false)
      // 把数据写入到csv中
      CsvUtil.saveCsv(dataFrame, "file:///E:\\jobrs\\csv")
    }
  }
}
