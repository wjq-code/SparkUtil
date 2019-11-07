package com.wjq.excel

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf}

class MyOutPutFormatExcel extends MultipleTextOutputFormat[Any, Any]{
  // 自定义文件的输出名称
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    // 业务逻辑规则
    val fileName = key.asInstanceOf[String]
    val file = fileName.split("_")(0)
    file.split("\\.")(0) + ".csv"
  }
  // 是否要输出key
  override def generateActualKey(key: Any, value: Any): Any = {
    // 如果需要输出key直接写key，如果不需要直接写null，value方法和这个方法相同
    null
  }
  // 是否输出value  根据业务来确定
  override def generateActualValue(key: Any, value: Any): Any = {
    value
  }
  // 检查输出目录是否存在，是否发生异常。。。。
  override def checkOutputSpecs(ignored: FileSystem, job: JobConf): Unit = {
    val outDir = FileOutputFormat.getOutputPath(job)
    if (outDir != null){
      FileOutputFormat.setOutputPath(job, outDir)
    }
  }
}
