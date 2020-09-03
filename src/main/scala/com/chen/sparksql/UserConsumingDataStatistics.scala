package com.chen.sparksql

import lombok.extern.log4j.Log4j
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * @author bystander
 * @date 2020/7/11
 */

case class User(userID: String, gener: String, age: Int, registerDate: String, role: String, region: String)

//define case class for consuming data
case class Order(orderID: String, orderDate: String, productID: Int, price: Int, userID: String)


object UserConsumingDataStatistics {

    def main(args: Array[String]): Unit = {

        Logger.getLogger("org").setLevel(Level.ERROR)

        val spark: SparkSession = SparkSession.builder()
            .appName("use cosuming data statistics")
            .master("local[8]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()

        val sqlCtx: SQLContext = spark.sqlContext
        val ctx: SparkContext = spark.sparkContext

//        spark.sparkContext.setLogLevel("WARN")

        import sqlCtx.implicits._

        //        convert user data to DataFrame and register it as a temp table
        val userDF: DataFrame = ctx.textFile("/Users/bystander/IdeaProjects/spark_test2/data/data20200711/sample_user_data.txt")
            .map(_.split(" "))
            .map(u => User(u(0), u(1), u(2).toInt, u(3), u(4), u(5))).toDF()

        userDF.createOrReplaceTempView("user")



        val orderDF: DataFrame = ctx.textFile("/Users/bystander/IdeaProjects/spark_test2/data/data20200711/sample_consuming_data.txt")
            .map(_.split(" "))
            .map(o => Order(o(0), o(1), o(2).toInt, o(3).toInt, o(4))).toDF()

        orderDF.createOrReplaceTempView("orders")

        userDF.persist(StorageLevel.MEMORY_ONLY_SER)
        orderDF.persist(StorageLevel.MEMORY_ONLY_SER)

        //The number of people who have orders in the year 2015
        val count: Long = orderDF.filter(orderDF("orderDate").contains("2015"))
            .join(userDF, orderDF("userID").equalTo(userDF("userID")))
            .count()
        println("The number of people who have orders in the year 2015:" + count)

        //total orders produced in the year 2014
        val countOfOrders2014: Long = sqlCtx.sql("select * from orders where orderDate like '2014%'").count()
        println("total orders produced in the year 2014:" + countOfOrders2014)

        //Calculate the max,min,avg prices for the orders that are producted by user with ID 10

        //        Orders that are produced by user with ID 1 information overview
        sqlCtx.sql("select o.orderID,u.userID from user u,orders o where u.userID = o.userID and u.userID = 1").show()
        println("Orders produced by user with ID 1 showed.")

        //Calculate the max,min,avg prices for the orders that are producted by user with ID 10
        val orderStatsForUser10: DataFrame = sqlCtx.sql("select max(o.price) as maxPrice,min(o.price) as minPrice,avg(o.price) as avgPrice,u.userID " +
            "FROM orders o,user u " +
            "where u.userID = 10 and u.userID = o.userID group by u.userID ")
        println("Order statistic result for usr with ID 10:")

        orderStatsForUser10.collect().map(order => "Minimum Price=" + order.getAs("minPrice")
            + ";Maximum Price=" + order.getAs("maxPrice")
            + ";Average Price=" + order.getAs("avgPrice")
        ).foreach(result => println(result))
    }

}
