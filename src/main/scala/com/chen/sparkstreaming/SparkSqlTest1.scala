package main.scala.com.chen.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author bystander
  * @date 2019/11/18
  */
object SparkSqlTest1 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkSqlTest1")

    val ssc = new StreamingContext(conf, Seconds(1))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))

    val wordCounts = pairs.reduceByKeyAndWindow((a:Int,b:Int) => (a+b),Seconds(30),Seconds(10))

    wordCounts.print()

    ssc.start()

    ssc.awaitTermination()
  }


}
