package com.chen.sparkcore

import com.chen.utils.JedisPoolUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * @author bystander
 * @date 2020/4/24
 */
object RedisSinkTest {

    var inputPath = "/Users/bystander/IdeaProjects/spark_test2/data/csv/student.csv"
    var redisUgi = ""
    var keyList = ""
    var mode = "insert"

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .appName("redis sink test")
            .master("local[*]")
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")


        putData(spark)

    }


    /**
     * 写入Redis
     *
     * @param dataIt
     * @param expireTime
     */
    def putDataPartition(dataIt: Iterator[(String, String)], expireTime: Int = 3600 * 24 * 2): Unit = {
        val jedisClient = JedisPoolUtils.getJedis()

        val dataList = dataIt.toArray

        val batchNum = 30
        val nStep = math.ceil(dataList.size / batchNum.toDouble).toInt

        for (index <- 0 to nStep) {
            val lowerIndex = batchNum * index
            val upperIndex =
                if (lowerIndex + batchNum >= dataList.size) {
                    dataList.size
                }
                else {
                    batchNum * (index + 1)
                }
            val batchData = dataList.slice(lowerIndex, upperIndex)
            var batchDataSize = 0
            val pipeline = jedisClient.pipelined()

            batchData.foreach(data => {
                val dataKey = data._1
                val dataValue = data._2
                pipeline.hset(dataKey, "field_name", dataValue)
                pipeline.expire(dataKey, expireTime)
            })
            pipeline.sync()
        }
    }

    /**
     * 批量插入数据
     *
     * @param spark
     */
    def putData(spark: SparkSession): Unit = {
        val rdd = spark
            .read
            .format("csv")
            .load(inputPath)
            .persist(StorageLevel.MEMORY_AND_DISK)


        println("count:" + rdd.count())
        println(rdd.take(5).mkString("\n"))

        //val redisUgiBc = spark.sparkContext.broadcast(redisUgi)


    }


}
