package com.wjq.udf

import java.util.UUID

import com.wjq.udf.IUDF
import com.wjq.udf.annons.UDF
import org.apache.spark.sql.SparkSession
import org.nutz.resource.Scans

import scala.collection.JavaConverters._
import scala.util.Random

case class kv(id:String ,value:Float ,time:String)
object UDFscans {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MysqlQueryDemo").master("local[*]").config("spark.driver.maxResultSize", "10g").getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    Scans.me()
      .scanPackage("com.wjq.udf.impl")
      .asScala
      .map(v =>(v.getAnnotation(classOf[UDF]).value() ,v.newInstance().asInstanceOf[IUDF[Any ,String]]))
      .foreach(v =>spark.udf.register(v._1 ,v._2.execute _))

    import spark.implicits._
    val df = sc.parallelize(List(
      kv(UUID.randomUUID().toString ,Random.nextFloat() ,"2019/10/22 15:50:01"),
      kv(UUID.randomUUID().toString ,Random.nextFloat() ,"2019/10/22 15:50:01"),
      kv(UUID.randomUUID().toString ,Random.nextFloat() ,"2019/10/22 15:50:01"),
      kv(UUID.randomUUID().toString ,Random.nextFloat() ,"2019/10/22 15:50:01"),
      kv(UUID.randomUUID().toString ,Random.nextFloat() ,"2019/10/22 15:50:01"),
      kv(UUID.randomUUID().toString ,Random.nextFloat() ,"2019/10/22 15:50:01"),
      kv(UUID.randomUUID().toString ,Random.nextFloat() ,"2019/10/22 15:50:01"),
      kv(UUID.randomUUID().toString ,Random.nextFloat() ,"2019/10/22 15:50:01")
    )).toDF()
      .createOrReplaceTempView("temp")

    spark.sql("select * from  temp").show(false)

    spark.sql("SELECT format_id(id) as id ,format_float(value) as value ,format_date(time) as time ,format_date_compact(time) as time1 from temp").show(false)

  }



}
