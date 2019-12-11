package com.wjq.udf.impl

import com.wjq.udf.IUDF
import com.wjq.udf.annons.UDF


@UDF("format_float")
class FloatFormatUDF extends IUDF[Float ,String]{
  /**
    * 函数处理
    */
  override def execute(value: Float): String = value.formatted("%.3f")
}
