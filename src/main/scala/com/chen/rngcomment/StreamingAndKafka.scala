package com.chen.rngcomment

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.PreferConsistent

/**
 * @author bystander
 * @date 2020/4/25
 */
object StreamingAndKafka {

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .appName("stream kafka")
            .master("local[2]")
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        val df: DataFrame = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
            .option("subscribe", "rng_comment")
            .load()

        df.writeStream
            .format("")

//
//        val conf: SparkConf = new SparkConf().setAppName("streaming kafka")
//            .setMaster("local[*]")
//
//        val sc = new SparkContext(conf)
//        sc.setLogLevel("WARN")
//        val ssc = new StreamingContext(sc, Seconds(5))

        //connect kafka


//        val kafkaParams = Map[String, Object](
//            "bootstrap.servers" -> "192.168.1.7:9092,192.168.1.7:9093,192.168.1.7:9094",
//            "key.deserializer" -> classOf[StringDeserializer],
//            "value.deserializer" -> classOf[StringDeserializer],
//            "group.id" -> "SparkKafkaDemo",
//            //earliest:当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
//            //latest:当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
//            //none:topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
//            //这里配置latest自动重置偏移量为最新的偏移量,即如果有偏移量从偏移量位置开始消费,没有偏移量从新来的数据开始消费
//            "auto.offset.reset" -> "latest",
//            //false表示关闭自动提交.由spark帮你提交到Checkpoint或程序员手动维护
//            "enable.auto.commit" -> (false: java.lang.Boolean)
//        )
//        val topic = Array("rng_comment")
//
//        //get data
//        val recordDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
//            LocationStrategy.Preferconsistent,
//            ConsumerStrategy.Subscribe[String, String](topic, kafkaParams))

        val url = "jdbc:mysql://localhost:3306/rng_comment?useUnicode=true&characterEncoding=utf8"
        val user = "root"
        val pwd = "root"

//        val result: DStream[Array[String]] = df.as[String].map(_.split(","))

//        val query: StreamingQuery = df.writeStream
//            .outputMode("complete")
//            .format("console")
//                    iter => {
//                        val connection: Connection = DriverManager.getConnection(url, user, pwd)
//                        var sql =
//                        """
//                              |insert into
//                              |vip_rank values
//                              |(?,?,?,?,?,?,?,?,?,?,?)
//                              |
//                              |""".stripMargin
//
//                        iter.foreach({
//                            lien => {
//                                val statement: PreparedStatement = connection.prepareStatement(sql)
//                                statement.setString(1,lien(0))
//                                statement.setString(2,lien(1))
//                                statement.setString(3,lien(2))
//                                statement.setString(4,lien(3))
//                                statement.setString(5,lien(4))
//                                statement.setString(6,lien(5))
//                                statement.setString(7,lien(6))
//                                statement.setString(8,lien(7))
//                                statement.setString(9,lien(8))
//                                statement.setString(10,lien(9))
//                                statement.setString(11,lien(10))
//                                statement.executeUpdate()
//                                statement.close()
//
//                            }
//                        })
//                        connection.close()
//                    }
//                })
//            }
//        })


    }

}
