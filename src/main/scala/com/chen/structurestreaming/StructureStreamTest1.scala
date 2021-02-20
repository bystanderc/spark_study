package com.chen.structurestreaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, SparkSession}

/**
 * @author bystander
 * @date 2020/10/19
 *
 */

case class Word(key:String, value:String, topic:String,partition:Integer,offset:Integer,timestamp:Timestamp,timestampType:Integer)
object StructureStreamTest1 {

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("structure streaming test1")
            .getOrCreate()

        val df: DataFrame = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "10.0.101.162:9092,10.0.101.162:9093,10.0.101.162:9094")
            .option("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
            .option("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
            .option("subscribe", "public")
            .load()

        import spark.implicits._

        val words: DataFrame = df.selectExpr("timestamp", "CAST(value AS STRING)")
            .as[(Timestamp, String)].toDF()



//        val windowedCounts = words
//            .withWatermark("timestamp", "10 minutes")
//            .groupBy(
//                window($"timestamp", "10 minutes", "5 minutes"),
//                $"word")
//            .count()


        val sq: StreamingQuery = words.writeStream
            .format("console")
            .trigger(Trigger.ProcessingTime("2 seconds"))
            .outputMode("append")
            .start()

        sq.awaitTermination()

    }

}
