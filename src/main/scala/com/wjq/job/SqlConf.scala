package com.wjq.job

import scala.beans.BeanProperty
import scala.collection.mutable.ListBuffer

class SqlConf{
  @BeanProperty
  var url:String = ""
  @BeanProperty
  var username:String = ""
  @BeanProperty
  var password:String = ""
  @BeanProperty
  var driver:String = ""
  @BeanProperty
  var table:ListBuffer[String] = null

  override def toString = s"SqlConf($url, $username, $password, $driver, $table)"
}

