package main.scala.com.chen.sparkstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * @author bystander
  * @date 2019/11/19
  */
object SqlNetWorkCount {

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN);

    val conf = new SparkConf().setMaster("local[2]").setAppName("SqlNetWorkCount")

    val ssc = new StreamingContext(conf, Seconds(10))

    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    val words = lines.flatMap(_.split(" "))

    words.foreachRDD { (rdd: RDD[String], time: Time) =>

      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)

      import spark.implicits._

      val wordDataFrame = rdd.map(w => Record(w)).toDF()

      wordDataFrame.createOrReplaceTempView("words")

      val wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word")

      println(s"========= $time =========")
      wordCountsDataFrame.show()


    }

    ssc.start()

    ssc.awaitTermination()


  }

}

case class Record(word: String)


/** Lazily instantiated singleton instance of SparkSession */
object SparkSessionSingleton {

  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}
