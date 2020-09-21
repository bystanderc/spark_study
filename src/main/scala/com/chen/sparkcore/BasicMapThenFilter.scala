package com.chen.sparkcore

import lombok.extern.log4j.Log4j
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author bystander
 * @date 2020/7/8
 */
object BasicMapThenFilter {

    def main(args: Array[String]): Unit = {
        val master = args.length match {
            case x: Int if x > 0 => args(0)
            case _ => "local"
        }

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("basic map then filter")
        val sc = new SparkContext(conf)

        sc.setLogLevel("WARN")

        val input: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6))
        val result: RDD[Int] = input.map(x => x * x)
            .filter(x => x != 1)

        println(result.collect().mkString(","))
    }

}
