package com.chen.structurestreaming

import org.apache.parquet.format.LogicalTypes.TimeUnits
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author bystander
 * @date 2020/9/10
 */
object StructureStreamingConsumeFromKafka extends  App {

    private val spark: SparkSession = SparkSession
        .builder()
        .master("local[*]")
        .appName("structure stream consume from kafka")
        .getOrCreate()

    private val kafkaDf: DataFrame = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "10.0.101.162:9092,10.0.101.162:9093,10.0.101.162:9094")
        .option("startingOffsets", "earliest")
        .option("subscribe", "public")
        .load()

    kafkaDf.writeStream
        .format("console")
        .trigger(Trigger.ProcessingTime("2 seconds"))
        .outputMode("append")
        .option("checkpointLocation","/Users/bystander/IdeaProjects/spark_test2/spark-checkpoint/StructureStreamingConsumeFromKafka/")
        .start

}
