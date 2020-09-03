package com.chen.structurestreaming

import java.sql.{Connection, DriverManager}

import org.apache.spark.sql.{ForeachWriter, Row}

/**
 * mysql输出类
 */
class MysqlSink(url: String, userName: String, pwd: String) extends ForeachWriter[Row] {
  //创建连接对象
  var conn: Connection = _

  //建立连接
  override def open(partitionId: Long, version: Long): Boolean = {
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection(url, userName, pwd)
    true
  }

  //数据写入mysql
  override def process(value: Row): Unit = {
    val p = conn.prepareStatement("replace into windowedCounts(window,word,count) values(?,?,?)")
    p.setString(1, value(0).toString)
    p.setString(2, value(1).toString)
    p.setString(3, value(2).toString)
    p.execute()
  }

  //关闭连接对象
  override def close(errorOrNull: Throwable): Unit = {
    conn.close()
  }
}