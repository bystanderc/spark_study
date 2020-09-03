package com.chen.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author bystander
 * @date 2020/7/8
 */
object BasicParseWholeCsv {

    def main(args: Array[String]): Unit = {

        //        if (args.length < 2) {
        //            println("Usage: [sparkmaster] [inputfile]")
        //            System.exit(1)
        //        }
        val master = "spark://localhost:7077"
        val inputFile = "/Users/bystander/IdeaProjects/spark_test2/data/csv/data1.txt"

        val conf: SparkConf = new SparkConf().setAppName("basic parse whole csv")
            .setMaster("local[4]")
            .setJars(List("/Users/bystander/IdeaProjects/spark_test2/target/spark-1.0-SNAPSHOT.jar"))

        val sc = new SparkContext(conf)

        val result: RDD[(String, Int)] = sc.textFile("/Users/bystander/IdeaProjects/spark_test2/data/csv/data1.txt")
            .flatMap(_.split(" "))
            .map { x => (x, 1) }
            .reduceByKey { _ + _}

        println(result.collect())

    }

}
