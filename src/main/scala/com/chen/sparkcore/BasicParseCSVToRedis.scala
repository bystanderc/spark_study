package com.chen.sparkcore

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @author bystander
 * @date 2018/12/21
 */
case class Person(name: String, favouriteAnimal: String)

object BasicParseCSVToRedis {

    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession
            .builder()
            .master("local[4]")
            .appName("BasicParseCSV")
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        val schema = StructType(Array(
            StructField("name", StringType, false),
            StructField("favoriteanimal", StringType, false)
        ))

        val inputFile: DataFrame = spark
            .read
            .format("csv")
            .schema(schema)
            .load("/Users/bystander/IdeaProjects/spark_test2/src/main/resources/favorite_animal.csv")


        inputFile.createOrReplaceTempView("animal")

        val result: DataFrame = spark.sql("select * from animal")

        result.show(5)

        result.foreach(result => {
            val config = new JedisPoolConfig
            config.setMaxTotal(20)
            config.setMaxIdle(20)
            config.setMinIdle(20)

            config.setTestOnBorrow(true)
            config.setTestOnReturn(true)
            val jedisPool = new JedisPool(config, "127.0.0.1", 6379, 1000 * 2)
            val jedisClient: Jedis = jedisPool.getResource
            jedisClient.setex(result.get(0).toString, 60, result.get(1).toString)
            jedisPool.close()
        })


        spark.stop()


    }


}
