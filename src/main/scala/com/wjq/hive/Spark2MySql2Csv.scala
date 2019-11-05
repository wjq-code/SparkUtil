package com.wjq.hive

import com.wjq.hive.outputFormat.MyOutPutFormat
import com.wjq.hive.partitioner.MyPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark2MySql2Csv {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sql2csv").setMaster("local[2]")
    // 构建sparkSession对象
    val sparkSession = SparkSession.builder()
      .config(conf)
      .config("spark.sql.warehouse", "spark-warehouse")
      .getOrCreate()
    val dataFrame:DataFrame = sparkSession.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url","jdbc:mysql://localhost:3306/test")
      .option("user","root")
      .option("password","root")
      .option("dbtable","test")
      .load()

    val rdd = dataFrame.rdd.map(row => {(row.toString(),1)}).repartition(1)
    // 前后partition的个数要一致

    val rsRDD = rdd.partitionBy(new MyPartitioner(1))

    // 把结果数据保存到hdfs中，并且按照自定义的文件名进行存储
    rsRDD.saveAsHadoopFile("hdfs目录", classOf[String], classOf[String], classOf[MyOutPutFormat])
  }
}
