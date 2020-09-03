package com.chen.structurestreaming

import org.apache.spark.sql.SparkSession

/**
 * @author bystander
 * @date 2020/2/3
 */
object StructureStreamingTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[2]").appName("structure stream test").getOrCreate()

    val lines = spark.readStream.format("socket").option("host", "localhost").option("port", "9999").load()

    import spark.implicits._

    val words = lines.as[String].flatMap(_.split(" "))

    val wordCount = words.groupBy("value").count()

    val query = wordCount.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()


  }

}
