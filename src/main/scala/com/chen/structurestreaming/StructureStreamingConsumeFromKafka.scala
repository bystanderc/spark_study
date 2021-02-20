package com.chen.structurestreaming

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author bystander
 * @date 2020/9/10
 */
object StructureStreamingConsumeFromKafka {


    def main(args: Array[String]): Unit = {
        val spark: SparkSession =
            SparkSession
                .builder()
                .master("local[*]")
                .appName("structure stream consume from kafka")
                .getOrCreate()

        val kafkaDf: DataFrame =
            spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.0.101.162:9092,10.0.101.162:9093,10.0.101.162:9094")
                .option("subscribe", "public")
                .load()

        val query: StreamingQuery = kafkaDf.writeStream
            .format("console")
            .trigger(Trigger.ProcessingTime("2 seconds"))
            .outputMode("append")
            .start()

        query.awaitTermination()
    }


}
