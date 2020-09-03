package main.scala.com.chen.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @author bystander
  * @date 2019/11/13
  */
object SparkSqlTest1 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("spark sql test")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val df: DataFrame = spark.read.json("path/test.json")

    df.show()
    df.printSchema()

    df.select("name").show()

    df.select($"name", $"age"+1).show()

    //按照年龄过滤
    df.filter($"age">11).show()

    //count people by age
    df.groupBy("age").count().show()



    df.createOrReplaceTempView("people")

    val sqlDF: DataFrame = spark.sql("select * from people")

    sqlDF.show()


    //Global Temporary View
    df.createOrReplaceTempView("people")

    spark.sql("SELECT * FROM global_temp.people").show()

    // Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.people").show()

    case class Person(name:String,age:Int)

    //val ds: Dataset[Person] = Seq(Person("Andy",12)).toDS()




  }




}
