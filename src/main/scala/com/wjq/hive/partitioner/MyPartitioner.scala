package com.wjq.hive.partitioner

import org.apache.spark.Partitioner

class MyPartitioner(num : Int) extends Partitioner{
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    // 根据key进行自定义分区条件
    val keyz = key.asInstanceOf[String]
    keyz.substring(0,2).toInt match {
      case 0 =>{
        0
      }
      case 1 =>{
        1
      }
      case 2 =>{
        2
      }
        //..................
    }
  }
}
