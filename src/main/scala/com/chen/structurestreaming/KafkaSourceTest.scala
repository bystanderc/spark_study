package com.chen.structurestreaming

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @author bystander
 * @date 2020/3/3
 */
object KafkaSourceTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .master("local[2]")
        .appName("kafka source test")
        .getOrCreate()

    // 订阅一个主题
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
      .option("subscribe", "partopic")
      .load()


    import spark.implicits._
    val value: Dataset[(String, String)] = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]


    //val query = value.writeStream.outputMode("update").format("console").start()

    val query1 = value
      .writeStream
      .format("csv")
      .option("path", "/Users/bystander/Downloads/kafkaTestOut")
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", "/Users/bystander/Downloads/kafkaTestCheckpoint")
      .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
      .start()


    query1.awaitTermination()
  }


  case class Words(word: String)

}
