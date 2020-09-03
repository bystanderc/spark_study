package com.chen.structurestreaming

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @author bystander
 * @date 2020/4/2
 */
object CanalKafkaTest {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .appName("CanalKafkaTest")
            .master("local[2]")
            .getOrCreate()


        val df = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "192.168.1.7:9092,192.168.1.7:9093,192.168.1.7:9094")
            .option("subscribe", "leyou")
            .load()


        val query = df.writeStream.format("console").start()

        query.awaitTermination()
    }

}
