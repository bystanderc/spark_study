package com.chen.sparksql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author bystander
 * @date 2020/7/9
 */
object SparkSqlFromMongo {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("myApp")
            .master("local[4]")
            .config("spark.mongodb.input.uri", "mongodb://localhost:27017/test.coll01")
            .config("spark.mongodb.output.uri", "mongodb://localhost:27017/test.coll")
            .getOrCreate()


        // 设置log级别
        spark.sparkContext.setLogLevel("WARN")


        val schema = StructType(
            List(
                StructField("_id", StringType),
                StructField("qty", StringType),
                StructField("type", StringType),
                StructField("money", StringType)
            )
        )

        // 通过schema约束，直接获取需要的字段
        val df = spark.read.format("com.mongodb.spark.sql").schema(schema).load()
        df.createOrReplaceTempView("fruit")
        val resDf = spark.sql("select * from fruit")
        resDf.show()

        //write to mongodb 方法1
//        val uriStr = "mongodb://lx:123456@172.16.18.26:20000/test1.test2?authSource=test1"
//        df.write.options(Map("spark.mongodb.output.uri"-> uriStr)).mode("overwrite").format("com.mongodb.spark.sql").save()

        //write to mongodb 方法2，配置对应的是spark.mongodb.output.uri
        resDf.write.mode("append").format("com.mongodb.spark.sql").save()


    }

}
