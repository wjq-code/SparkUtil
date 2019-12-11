package com.wjq.udf

trait IUDF[P,R] extends Serializable {
  /**
    * 函数处理
    */
  def execute(value:P):R ;

}
