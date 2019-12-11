package com.wjq.excel

import java.util
import java.util.{Collections, Comparator, Map}

import com.wjq.csv.CsvUtil
import org.apache.flink.table.api.scala.e
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import shapeless.ops.nat.LT.<


object ExcelUtil {

  val peopleSchema = StructType(Array(
    StructField("id", IntegerType, nullable = true),
    StructField("name", StringType, nullable = true),
    StructField("age", StringType, nullable = true),
    StructField("sex", StringType, nullable = true)
  ))

  def getExcelUtil(sparkSession: SparkSession, path: String) = {
    //    println(path)
    val df = sparkSession.read
      //      .format("com.crealytics.spark.excel")
      //      .option("sheetName", "Sheet1")
      //      .option("useHeader", "true")
      //      .schema(peopleSchema)
      //      .load("path")
      //
      //
      //    df.show()

      /*第二种方法，不指定schema,自动判断*/
      .format("com.crealytics.spark.excel")
      .option("useHeader", "true") // 是否将第一行作为表头
      .option("inferSchema", "false") // 是否推断schema
      .option("workbookPassword", "None") // excel文件的打开密码

      .load(path) //excel文件路径
    // 先堆地区进行分组找出每个地区的调查对象
    //    val groupByAddr = df.groupBy("station").count()
    // 按月份进行分组统计每个月下患病人数
    val rdd = df.rdd
    val all = df.groupBy("station").count().rdd.map(row => {(row.getString(0),row.getLong(1))})
    val order = rdd.filter(row => {
      val date2011 = row.getString(9)
      if (date2011 == "1.yes") {
        true
      } else {
        false
      }
    })
    order.map(row => {
      (row.getString(1), row)
    }).groupByKey().map(row => {
      val map = new util.HashMap[String, Integer]()
      var sb:StringBuffer =null
      var month = ""
      var addr = ""
      var list:util.ArrayList[java.util.Map.Entry[String, Integer]] = null
      for (elem <- row._2) {
        sb =  new StringBuffer()
        addr = elem.getString(1)
        val x = elem.getString(2)
        val y = elem.getString(3)
        try {
          month = elem.getString(6)
        } catch {
          case e: Exception => month = ""
        }
        var count = 0
        if (month != "" && month != "null") {
          if (!map.containsKey(month)) {
            count = 1
            map.put(month, count)
          } else {
            count = map.get(month)
            map.put(month, count + 1)
          }
        }
        list = new util.ArrayList(map.entrySet)
        //(o1:java.util.Map.Entry[String, Integer],o2:java.util.Map.Entry[String, Integer]) -> (o1.getValue() - o2.getValue())
        Collections.sort(list, new Comparator[java.util.Map.Entry[String, Integer]]() {
          override def compare(o1: Map.Entry[String, Integer], o2: Map.Entry[String, Integer]): Int = {
            //升序排列
            //            return o1.getValue - o2.getValue
            //降序排列
            return (o2.getValue() - o1.getValue());
          }
          list.get(0)
        })
        sb.append(addr).append("\t").append(x).append("\t").append(y).append("\t")
      }
      sb.append("2011/" +list.get(0).getKey).append("\t").append(list.get(0).getValue)
//      println(sb.toString)
      (row._1, sb)
    }).join(all).map(row => {
      row._2._1.toString + "\t" + row._2._2
    }).repartition(1).saveAsTextFile("file:///D:\\idea_workspace\\SparkUtil\\File\\2015")

    //    CsvUtil.saveCsv(df, "hdfs://hadoop:9000/csv/201911066")
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val path = "file:///D:\\idea_workspace\\SparkUtil\\File\\sat_月(1).xlsx"
    val sparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    getExcelUtil(sparkSession, path)


  }
}
