package com.chen.structurestreaming

import java.sql.Timestamp

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @author bystander
 * @date 2020/9/10
 */
object StructureStreamingFromText extends  App {

    private val spark: SparkSession = SparkSession
        .builder()
        .master("local[*]")
        .appName("structure streaming from socket")
        .getOrCreate()

    private val textDF = spark
        .read
        .text("/Users/bystander/IdeaProjects/spark_test2/data/data20200711/sample_user_data.txt")


    textDF
        .repartition(100)
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers","10.0.101.162:9092,10.0.101.162:9093,10.0.101.162:9094")
        .option("checkpointLocation", "/Users/bystander/IdeaProjects/spark_test2/spark-checkpoint/")
        .option("topic","public")
        .save()


//    private val wordCounts: DataFrame = words.groupBy(
//        window($"timestamp", "10 minutes", "5 minutes"),
//        $"word"
//    ).count()

//    val query: StreamingQuery = wordCounts
//        .writeStream
//        .outputMode("update")
//        .format("console")
//        .start()

//    query.awaitTermination()





}
