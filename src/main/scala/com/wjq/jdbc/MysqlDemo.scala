package com.wjq.jdbc

import java.util.Date

import com.wjq.schema.SchemaUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

object MysqlDemo {
  def main(args: Array[String]): Unit = {

    var map: Map[String, String] = Map()
    map.+("" -> "")
    //print(schema)
    val spark = SparkSession.builder().appName("MysqlQueryDemo").master("local[*]").config("spark.driver.maxResultSize", "10g").getOrCreate()

    val url = "jdbc:mysql://localhost:3306/nutzsite?zeroDateTimeBehavior=convertToNull"
    val tableName = "sys_menu"
    var partion = new Array[String](1)
    // 设置连接用户&密码
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    var jdbcDF = spark.read.jdbc(url, s"(select count(1) from  ${tableName}) t", partion, prop)


    // jdbcDF = jdbcDF.repartition(19)
    val count: Long = jdbcDF.first().get(0).toString.toLong


    println("====================================================" + count.toString)

    println("======================================================" + jdbcDF.rdd.partitions.size)

    val tuple = SchemaUtil.partition(count, partion)
    partion = new Array[String](tuple._1)
    for (i <- 0 until partion.size) {
      partion(i) = s"1=1 limit ${i * tuple._2},${tuple._2}"
    }
    //Thread.sleep(200000)
    //spark.sql()


    jdbcDF = spark.read.jdbc(url ,s"(SELECT (@rowNum:=@rowNum+1) AS rowNo ,${tableName}.* FROM ${tableName}, (SELECT (@rowNum :=0)) b ORDER BY rowNo ASC) t" ,prop)

    jdbcDF = jdbcDF.repartition(tuple._1)

    //    jdbcDF = spark.read.jdbc(url, s"(SELECT (@rowNum:=@rowNum+1) AS rowNo ,${tableName}.* FROM ${tableName}, (SELECT (@rowNum :=0)) b ORDER BY rowNo ASC) t", partion, prop)
    //     .drop("rowNo")

    //    jdbcDF = spark.read.jdbc(url, tableName, prop).repartition(20)
    //
    //    //jdbcDF = spark.read.jdbc(url, tableName, partion, prop)
    val at = jdbcDF.rdd.glom().collect()
    for (elem <- at) {
      println("elem.length=====================================================" + elem.length)
    }
    println("====================================================" + count)
    println("======================================================" + jdbcDF.rdd.partitions.size)
    //
    //    jdbcDF.show(2, false)
    //    //    print(new Date())
    //
    val tong = SchemaUtil.createDataFrame(spark, jdbcDF)

    //tong.write.format("csv").option("delimiter",",").option("header" ,"true").save(s"file:///d:/mr/tong3")


    //    import org.apache.spark.sql.functions._
    //    import spark.implicits._
    //    val result: RDD[(String, Int)] = jdbcDF.filter(length(jdbcDF("sfzh")) === 18)
    //      .rdd.map(rdd => {
    //      ("`" + rdd.get(0), 1)
    //    })
    //    .partitionBy(new MyPartition(3))
    //
    //    result.toDF("sfzh", "num")
    //      .select("sfzh")
    //      .write
    //      .format("csv")
    //      //.option("header", "true")
    //      .option("inferSchema", "false")
    //      .save("file:///d:/mr/data11111.csv")
  }

  def saveCsv(data: DataFrame, path: String): Unit = {
    data.write.format("csv").option("header", "true").save(path)
  }


  case class User(id: Int, studentid: Int, name: String, age: Int, sex: String, birthday: String)

}
