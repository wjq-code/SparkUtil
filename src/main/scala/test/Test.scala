package test

import com.wjq.collection.GlobalConstant
import com.wjq.csv.CsvUtil
import com.wjq.excel.ExcelUtil
import com.wjq.jdbc.MySqlUtil
import com.wjq.job.SqlConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("WARN")
    val sparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()
//
//    val mysqlConf = new SqlConf()
//    mysqlConf.setUrl("jdbc:mysql://localhost:3306/test")
//    mysqlConf.setUrl("root")
//    mysqlConf.setPassword("root")
//    mysqlConf.setDriver(GlobalConstant.MYSQL_DRIVER)
//    var list = new ListBuffer[String]
//    list.+=("test")
//    mysqlConf.setTable(list)
//    /*
//    println(mysqlConf.toString)
//    MySqlUtil.getMysqlDate(sparkSession, mysqlConf)
//    val path = "hdfs://hadoop:9000/csv/20131104"
//    CsvUtil.readCsv(sparkSession, path)
//    */
    val test = "aa.csv"
      print(test.split("\\.")(0))

    CsvUtil.getCsvData(sparkSession,"").show()
  }
}
