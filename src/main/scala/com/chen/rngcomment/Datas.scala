package com.chen.rngcomment

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Comment(
                      indes: String,
                      child_comment: String,
                      comment_time: String,
                      content: String,
                      da_v: String,
                      like_status: String,
                      pic: String,
                      user_id: String,
                      user_name: String,
                      vip_rank: String,
                      stamp: String)

object Datas {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder().appName("rng comment test").master("local[*]").getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        val comment = new StructType(Array(
            StructField("indes", StringType, true),
            StructField("child_comment", StringType, true),
            StructField("comment_time", StringType, true),
            StructField("content", StringType, true),
            StructField("da_v", StringType, true),
            StructField("like_status", StringType, true),
            StructField("pic", StringType, true),
            StructField("user_id", StringType, true),
            StructField("user_name", StringType, true),
            StructField("vip_rank", StringType, true),
            StructField("stamp", StringType, true)
        ))

        val datas: DataFrame = spark.read
            .format("csv")
            .option("seq", "\t")
            .load("/Users/bystander/Downloads/rng_comment.csv")

        import spark.implicits._

        val ds: Dataset[Comment] = datas
            .as[String]
            .flatMap(_.split("\r"))
            .filter(_.split("\t").length == 11)
            .map(x => {
                val arr: Array[String] = x.split("\t")
                new Comment(arr(0), arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7), arr(8), arr(9), arr(10))
            })


        //ds.createOrReplaceTempView("comment")

        //val new_rng_comment: DataFrame = spark.sql("select * from comment where indes is not null")

//        保存到MySQL中
        ds.write
            .format("jdbc")
            .option("url", "jdbc:mysql://10.0.0.176:4000/test")
            .option("dbtable", "test.rng_comment")
            .option("user", "root")
            .option("password", "Passw0rd")
            .mode("overwrite")
            .save()
//        ds.write
//            .format("csv")
//            .option("path","/Users/bystander/IdeaProjects/spark_test2/data/comment/rng_comment.csv")
//            .option("filename","new_rng_comment")
//            .mode("overwrite")
//            .save()


    }
}
