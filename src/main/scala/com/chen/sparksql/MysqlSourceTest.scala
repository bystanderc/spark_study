package com.chen.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author bystander
 * @date 2020/3/6
 */

case class tb_user(id: Integer, username: String, password: String, phone: String, created: String, salt: String)

case class tb_sku(id: Integer, spuId: Integer, title: String, images: String, price: BigDecimal, indexese: String, ownSpec: String, enable: Int, create_time: String)

object MysqlSourceTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .master("spark://localhost:8127")
        .appName("mysql source test")
        .getOrCreate()

    val frame: DataFrame = spark
        .read
        .format("jdbc")
        .option("url", "jdbc:mysql://10.31.194.51:3306/leyou")
        .option("dbtable", "tb_sku")
        .option("user", "root")
        .option("password", "root")
        .load()

    frame.createOrReplaceTempView("tb_sku")

    val sqlDf = spark.sql("select id,spu_id,title,images,price,indexes,own_spec,enable,create_time,last_update_time from tb_sku")


    //    sqlDf.show()


    sqlDf.write
      .format("jdbc")
      .option("url", "jdbc:mysql://10.31.194.51:3306/leyou?useUnicode=true&characterEncoding=utf8")
      .option("dbtable", "leyou.tb_sku2")
      .option("user", "root")
      .option("password", "root")
      .mode("append")
      .save()

  }

}
