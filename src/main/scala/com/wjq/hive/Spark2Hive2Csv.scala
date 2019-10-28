package com.wjq.hive

import com.wjq.hive.outputFormat.MyOutPutFormat
import com.wjq.hive.partitioner.MyPartitioner
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark2Hive2Csv {
  def main(args: Array[String]): Unit = {

    // 构建sparkSession对象
    val sparkSql = SparkSession.builder()
      .appName("hive2csv")
      .config("spark.sql.warehouse", "spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 获取hive中的数据
    val dF: DataFrame = sparkSql.sql("sql")

    // 数据清洗
    val filterRDD = dF.rdd.filter(row => true)

    /**
     * 对数据的业务操作
     */

    val mapRDD = filterRDD.map(row => {
      (row.getString(0), 1)
    }).repartition(10)
    // 前后partition的个数要一致
    val rsRDD = mapRDD.partitionBy(new MyPartitioner(10))

    // 把结果数据保存到hdfs中，并且按照自定义的文件名进行存储
    rsRDD.saveAsHadoopFile("hdfs目录", classOf[String], classOf[String], classOf[MyOutPutFormat])
  }
}
