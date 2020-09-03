//package com.chen.sparkcore
//
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//import org.codehaus.jackson.map.{DeserializationConfig, ObjectMapper}
//
///**
// * @author bystander
// * @date 2020/7/8
// */
//case class Person(name: String, lovesPandas: Boolean) // Note: must be a top level class
//
//object BasicParseJsonWithJackson {
//
//    def main(args: Array[String]): Unit = {
//        if (args.length < 3) {
//            println("Usage: [sparkmaster] [inputfile] [outputfile]")
//        }
//
//        val master = args(0)
//        val inputFile = args(1)
//        val outputFile = args(2)
//
//
//        val conf: SparkConf = new SparkConf().setAppName("BasicParseJsonWithJackson")
//            .setMaster(master)
//            .setJars(List("/Users/bystander/IdeaProjects/spark_test2/target/spark-1.0-SNAPSHOT.jar"))
//
//        val sc = new SparkContext(conf)
//
//        val input: RDD[String] = sc.textFile(inputFile)
//
//        val result: RDD[Person] = input.mapPartitions(records => {
//
//            val mapper = new ObjectMapper() with ScalaObjectMapper
//            mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false)
//            mapper.registerModule(DefaultScalaModule)
//
//            records.flatMap(record => {
//                try {
//                    Some(mapper.readValue(record, classOf[Person]))
//                } catch {
//                    case e: Exception => None
//                }
//
//            })
//        }, true)
//
//        result.filter(_.lovesPandas).mapPartitions(records => {
//            val mapper = new ObjectMapper with ScalaObjectMapper
//            mapper.registerModule(DefaultScalaModule)
//            records.map(mapper.writeValueAsString(_))
//        })
//            .saveAsTextFile(outputFile)
//
//
//    }
//
//}
