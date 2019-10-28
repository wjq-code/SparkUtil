package com.wjq.hive.outputFormat

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

class MyOutPutFormat extends MultipleTextOutputFormat[Any, Any]{
  // 自定义文件的输出名称
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    // 业务逻辑规则
    // 按照身份证的前两位进行输出，对相同省份的数据存储到一个文件中
    val keyz = key.asInstanceOf[String]
    val xzbh = keyz.substring(0,2).toInt
    xzbh match {
      case 34 =>{
        xzbh + "0000" + ".csv"
      }
        //...............
    }
  }
  // 是否要输出key
  override def generateActualKey(key: Any, value: Any): Any = {
    // 如果需要输出key直接写key，如果不需要直接写null，value方法和这个方法相同
    key
  }
  // 是否输出value  根据业务来确定
  override def generateActualValue(key: Any, value: Any): Any = {
    null
  }
  // 检查输出目录是否存在，是否发生异常。。。。
  override def checkOutputSpecs(ignored: FileSystem, job: JobConf): Unit = {
    val outDir = FileOutputFormat.getOutputPath(job)
    if (outDir != null){
      FileOutputFormat.setOutputPath(job, outDir)
    }
  }
}
