package com.chen.structurestreaming

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * @author bystander
 * @date 2020/3/7
 */
object SocketSourceTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]")
      .appName("socket source test")
      .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .option("includeTimestamp", true)
      .load()

    // Split the lines into words, retaining timestamps
    val words = lines.as[(String, Timestamp)].flatMap(line =>
      line._1.split(" ").map(word => (word, line._2))
    ).toDF("word", "timestamp")

    // Group the data by window and word and compute the count of each group
    val windowedCounts = words.withWatermark("timestamp", "2 minutes").groupBy(
      window($"timestamp", "1 minutes", "30 second"), $"word"
    ).count().orderBy("window")

    // Start running the query that prints the windowed word counts to the console
    //    val query = windowedCounts.writeStream
    //      .outputMode("complete")
    //      .format("console")
    //      .option("truncate", "false")
    //      .start()

    //创建流失数据写入格式对象
    val mysqlSink = new MysqlSink("jdbc:mysql://localhost:3306/leyou?useUnicode=true&characterEncoding=utf8", "root", "root")

    //再将处理好的数据重新写入到kafka中输出
    val query = windowedCounts.writeStream
      .outputMode("complete") //三种模式：append、complete、update
      //检查点必须设置，不然会报错
      .option("checkpointLocation", "/Users/bystander/IdeaProjects/spark_test2/spark-checkpoint/")
      .foreach(mysqlSink) //输出到mysql
      .start()

    query.awaitTermination()
  }

}
