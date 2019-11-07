package com.wjq.json

import com.wjq.csv.CsvUtil
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

object SparkJson {
  def getJsonData(
                   sparkSession: SparkSession,
                   path: String): DataFrame = {
    sparkSession.read
      .format("json")
      .load(path)
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("SparkXmlTest")
      .master("local[*]")
      .getOrCreate()
    val path = "file:///D:\\idea_workspace\\SparkUtil\\File\\test.json"
    //
    val df = getJsonData(sparkSession, path)
    //        df.printSchema()

    println(df.schema)
    val peopleSchema = StructType(Array(
      StructField("id", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("age", StringType, nullable = true),
      StructField("sex", StringType, nullable = true)
    ))
    // 原始数据的schema
//    val sourceSchema = df.schema
//    for (elem <- sourceSchema) {
//      if (elem.dataType != StringType) {
//        elem.
//      }
//    }


    import sparkSession.implicits._
    val df1 = df.rdd.map { r =>
//      val info = r.getAs[mutable.WrappedArray[String]](2)
//      val infos = info.mkString(",")
//      Row(r(0).toString, r(1).toString, r(2).toString, r(3).toString)
      NewSchema(r(0).toString, r(1).toString, r(2).toString, r(3).toString)
    }.toDF()
//    val ds = sparkSession.createDataFrame(df1,peopleSchema)
    df1.write.json("file:///D:\\idea_workspace\\SparkUtil\\File\\json2json")

    //    df1.show()
//    CsvUtil.saveCsv(df1, "file:///D:\\idea_workspace\\SparkUtil\\File\\json2csv")


//    CsvUtil.getCsvData(sparkSession,"file:///D:\\idea_workspace\\SparkUtil\\File\\json2csv\\part-00000-a6ceb995-8324-4790-88ea-c18762f6fcc4-c000.csv").show(false)
  }

  case class NewSchema(city: String, nlcd_name: String, infos: String, country: String)

}
