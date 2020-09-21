package com.chen.structurestreaming.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author bystander
 * @date 2020/4/23
 */

case class JDBCConnectionConfig(url: String, dbtable: String, user: String, password: String)

object StructureStreamingTest3 {

    def main(args: Array[String]): Unit = {

        val URL = "jdbc:mysql://localhost:3306/leyou?characterEncoding=UTF-8"
        val DBTABLE = "tb_spu"
        val DBTABLE2 = "tb_brand"
        val USER = "root"
        val PASSWORD = "root"

        //Logger.getRootLogger.setLevel(Level.WARN)

        val spark = SparkSession.builder()
            .master("local[*]")
            .appName("structure streaming test03")
            .getOrCreate()

        val context = spark.sparkContext

        context.setLogLevel("WARN")

        val config =  JDBCConnectionConfig(URL,DBTABLE,USER,PASSWORD)

        val jdbcDFspu: DataFrame = spark
            .read
            .format("jdbc")
            .option("url", config.url)
            .option("dbtable", config.dbtable)
            .option("user", config.user)
            .option("password", config.password)
            .load()

        val frameDF = jdbcDFspu.toDF()

        frameDF.createOrReplaceTempView("spu")


        val jdbcDFbrand: DataFrame = spark
            .read
            .format("jdbc")
            .option("url", URL)
            .option("dbtable", DBTABLE2)
            .option("user", USER)
            .option("password", PASSWORD)
            .load()

        val frameDF2 = jdbcDFbrand.toDF()

        frameDF2.createOrReplaceTempView("brand")

        spark.sql("select a.id,a.title,b.name from spu a join brand b on a.brand_id = b.id").show(100)

    }

}
