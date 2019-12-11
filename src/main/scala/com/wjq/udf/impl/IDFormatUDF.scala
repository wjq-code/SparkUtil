package com.wjq.udf.impl

import com.wjq.udf.IUDF
import com.wjq.udf.annons.UDF


@UDF("format_id")
class IDFormatUDF extends IUDF[String ,String]{
  /**
    * 函数处理
    */
  override def execute(value: String): String = value.replaceAll("-" ,"")
}
