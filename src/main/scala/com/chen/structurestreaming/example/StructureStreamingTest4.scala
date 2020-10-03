package com.chen.structurestreaming.example

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author bystander
 * @date 2020/4/22
 */

//请使用Structured Streaming读取student_info文件夹写的csv文件
object StructureStreamingTest4 {
    def main(args: Array[String]): Unit = {
        val session = SparkSession.builder().appName("structure streaming test4")
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
        val StudentDF: DataFrame = session.read
            .schema(studentType)
            .csv("/Users/bystander/IdeaProjects/spark_test2/data/csv/student2.csv")

        import session.implicits._

        //2.1统计出文件中的男女生各有多少人

        StudentDF.createOrReplaceTempView("student")

        val df: DataFrame = session.sql("select distinct id as maxId from student")


        val dts: Array[String] = df.collect().map(_.get(0).toString)

       for (dt <- dts){
           println(dt)
       }
    }

}
