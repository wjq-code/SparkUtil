package test

import java.text.DecimalFormat

import com.wjq.collection.GlobalConstant
import com.wjq.csv.CsvUtil
import com.wjq.jdbc.MySqlUtil2
import com.wjq.job.SqlConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object Test {
  def main(args: Array[String]): Unit = {
    val format = new DecimalFormat("0000")
    val a:Long = 101;
    println(a.toString)
//    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
//    //    val sc = new SparkContext(conf)
//    //    sc.setLogLevel("WARN")
//    val sparkSession = SparkSession.builder()
//      .config(conf)
//      .getOrCreate()
//    //
//    val mysqlConf = new SqlConf()
//    mysqlConf.setUrl("jdbc:mysql://localhost:3306/test")
//    mysqlConf.setUrl("root")
//    mysqlConf.setPassword("root")
//    mysqlConf.setDriver(GlobalConstant.MYSQL_DRIVER)
//    var list = new ListBuffer[String]
//    list.+=("data")
//    mysqlConf.setTable(list)
//
////    MySqlUtil2.getMysqlDate(sparkSession, mysqlConf)
//    CsvUtil.getCsvData(sparkSession, "file:///E:\\jobrs\\csv1").show(false)
//    /*
//
//    println(mysqlConf.toString)
//    MySqlUtil.getMysqlDate(sparkSession, mysqlConf)
//    val path = "hdfs://hadoop:9000/csv/20131104"
//    CsvUtil.readCsv(sparkSession, path)
//    */
//    //    val test = "aa.csv"
//    //    print(test.split("\\.")(0))
//    //
//    //    CsvUtil.getCsvData(sparkSession, "").show()
//
//    //    var conn: Conn = null
//    //
//    //    conn = new Conn1("12215","22")
//    //    println(conn.id)
//    //    println(conn.getClass)
  }
}

class Conn(val id: String)

class Conn1(override val id: String, name: String) extends Conn(id)

//class Conn2(id: String, name: String) extends Conn
