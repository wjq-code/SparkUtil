package com.wjq.schema

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType


object SchemaUtil {
  def createDataFrame(spark: SparkSession ,inputDataFrame:DataFrame): DataFrame ={
    var outputDataFrame  = inputDataFrame
    import org.apache.spark.sql.functions._

    //创建udf函数 处理array类型的数据
    val arrayToStr = udf((str: Any) => {
      str match {
        case p:Seq[StructType] =>str.asInstanceOf[Seq[StructType]].mkString(",")
        case p:Seq[String] =>str.asInstanceOf[Seq[String]].mkString(",")
        case p:String => str.toString
        case _ => null // 匹配不到为null
      }
    })

    outputDataFrame.schema.foreach(structname =>{
      outputDataFrame.printSchema()
      if(structname.dataType.simpleString.startsWith("array")){
        outputDataFrame = outputDataFrame.withColumn(structname.name ,arrayToStr(outputDataFrame(structname.name)))
      }else if(structname.dataType.simpleString.startsWith("struct")){
        outputDataFrame = outputDataFrame.withColumn(structname.name ,to_json(outputDataFrame(structname.name)))
      }else if (structname.dataType.simpleString.startsWith("map")) {
        outputDataFrame = outputDataFrame.withColumn(structname.name, to_json(col(structname.name)))
      }
    })
    outputDataFrame
  }

}
