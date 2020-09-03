package com.chen.structurestreaming.example

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession, DataFrame,Row}

/**
 * @author bystander
 * @date 2020/4/22
 */

//请使用Structured Streaming读取student_info文件夹写的csv文件
object StructureStreamingTest2 {
    def main(args: Array[String]): Unit = {
        val session = SparkSession.builder().appName("structure streaming test2")
            .master("local[*]")
            .getOrCreate()

        val context = session.sparkContext

        context.setLogLevel("WARN")

        val studentType = new StructType()
            .add("id", "string")
            .add("name", "string")
            .add("sex", "string")
            .add("idStudent", "string")
            .add("date", "string")

        //接收数据
        val StudentDF: DataFrame = session.readStream
            .schema(studentType)
            .csv("/Users/bystander/IdeaProjects/spark_test2/data/csv/")

        import session.implicits._

        //2.1统计出文件中的男女生各有多少人

        val sex: Dataset[Row] = StudentDF.selectExpr("sex").groupBy("sex").count().sort("count")


        //2.2统计出姓“王”男生和女生的各有多少人
        val wang = StudentDF
            .selectExpr("name", "sex")
            .where("name like '王%'")
            .groupBy("sex")
            .count()
            .sort($"count".desc)

        //输出数据
        wang.writeStream
            .format("console")
            .outputMode("complete")
            .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
            .start()
            .awaitTermination()
    }

}
