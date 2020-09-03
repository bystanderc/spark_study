package com.chen.structurestreaming.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author bystander
 * @date 2020/4/23
 */
object StructureStreamingTest3 {

    def main(args: Array[String]): Unit = {

        //Logger.getRootLogger.setLevel(Level.WARN)

        val spark = SparkSession.builder().master("local[*]").appName("structure streaming test03").getOrCreate()

        val context = spark.sparkContext

        context.setLogLevel("WARN")

        val jdbcDFspu: DataFrame = spark
            .read
            .format("jdbc")
            .option("url", "jdbc:mysql://localhost:3306/leyou?characterEncoding=UTF-8")
            .option("dbtable", "tb_spu")
            .option("user", "root")
            .option("password", "root")
            .load()

        val frameDF = jdbcDFspu.toDF()

        frameDF.createOrReplaceTempView("spu")


        val jdbcDFbrand: DataFrame = spark
            .read
            .format("jdbc")
            .option("url", "jdbc:mysql://localhost:3306/leyou?characterEncoding=UTF-8")
            .option("dbtable", "tb_brand")
            .option("user", "root")
            .option("password", "root")
            .load()

        val frameDF2 = jdbcDFbrand.toDF()

        frameDF2.createOrReplaceTempView("brand")

        spark.sql("select a.id,a.title,b.name from spu a join brand b on a.brand_id = b.id").show(100)

    }

}
