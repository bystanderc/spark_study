package com.chen.sparkstreaming

import com.alibaba.fastjson.JSON
import lombok.extern.log4j.Log4j
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}

/**
 * @author bystander
 * @date 2020/9/7
 */
@Log4j
object SparkStreamingFromKafka extends  App {

    Logger.getLogger("org").setLevel(Level.ERROR)


    private val spark: SparkSession = SparkSession
        .builder()
        .master("local[*]")
        .appName("spark streaming from kafka")
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    private val df: DataFrame = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "10.0.101.162:9092,10.0.101.162:9093,10.0.101.162:9094")
        .option("checkpointLocation", "/Users/bystander/IdeaProjects/spark_test2/spark-checkpoint/")
        .option("subscribe", "public")
        .load()


    val query = df
        .writeStream
        .outputMode("append")
        .format("console")
        .start()

    query.awaitTermination()



}
