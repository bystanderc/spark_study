package com.chen.structurestreaming.example

import java.io.FileOutputStream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
 * @author bystander
 * @date 2020/9/21
 */
object StructureStreamingFromSocket {


    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("stucture streaming from socket")
            .getOrCreate()

        val lines: DataFrame = spark.readStream
            .format("socket")
            .option("host", "localhost")
            .option("port", "9999")
            .load()

        import spark.implicits._

        // Split the lines into words
        val words = lines.as[String].flatMap(_.split(" "))

        // Generate running word count
        val wordCounts = words.groupBy("value").count()


        val query = wordCounts.writeStream
            .outputMode("complete")
            .format("console")
            .start()

        query.awaitTermination()

    }

}
