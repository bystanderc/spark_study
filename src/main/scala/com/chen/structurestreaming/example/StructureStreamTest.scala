package com.chen.structurestreaming.example

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @author bystander
 * @date 2020/4/21
 */


//使用Structured Streaming读取Socket数据，把单词和单词的反转组成 json 格式写入到当前目录中的file文件夹中
object StructureStreamTest {
    def main(args: Array[String]): Unit = {
        //        创建sparksession
        val spark = SparkSession.builder().master("local[*]").appName("Structure Stream Test").getOrCreate()
        val sc = spark.sparkContext
        sc.setLogLevel("WARN")

        val socket = spark.readStream
            .option("host", "localhost")
            .option("port", "9999")
            .format("socket")
            .load()

        import spark.implicits._
        val dataDS: Dataset[String] = socket.as[String]
        val df = dataDS.flatMap(_.split(" "))
            .map({ x => (x, x.reverse) })
            .toDF("before", "reverse")


        df.writeStream
            .format("json")
            .option("path","/Users/bystander/IdeaProjects/spark_test2/result/StructureStreamingTest/")
            .option("checkpointLocation","/Users/bystander/IdeaProjects/spark_test2/result/StructureStreamingTestCheckPointLocation/")
            .trigger(Trigger.ProcessingTime(1000))
            .start()
            .awaitTermination()

    }


}
