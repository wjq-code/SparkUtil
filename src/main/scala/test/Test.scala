package test

import java.sql.Timestamp
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util
import java.util.{Calendar, Date}

import com.wjq.collection.GlobalConstant
import com.wjq.csv.CsvUtil
import com.wjq.jdbc.MySqlUtil2
import com.wjq.job.SqlConf
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.nutz.lang.Strings

import scala.collection.mutable.ListBuffer

object Test {
  def main(args: Array[String]): Unit = {
    val cal = Calendar.getInstance()
    cal.set(2020,2,0)
    println(cal.getActualMaximum(Calendar.DAY_OF_MONTH))
    println("202002".substring(2))


    //c4ca4238a0b923820dcc509a6f75849b
    //c4ca4238a0b923820dcc509a6f75849b

//    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val tim = df.format(new Date())
//    val time = new Timestamp(new Date().getTime).toString.substring(0,19)
//    println(time)
//  val str = ""
//    println((str.split(",")(0))== "")x`
//
//    val list:util.ArrayList[String] = new util.ArrayList[String]
//    list.add("1")
//    list.add("2")
//    println(list.toString)

//    val format = new DecimalFormat("0000")
//    val a:Long = 101;
//    println(a.toString)
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
//    list.+=("biz_log_isddl1")
//    mysqlConf.setTable(list)
//
//    MySqlUtil2.getMysqlDate(sparkSession, mysqlConf)
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
