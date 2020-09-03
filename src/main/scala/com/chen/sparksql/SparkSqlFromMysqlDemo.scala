package com.chen.sparksql

import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * @author bystander
 * @date 2020/7/8
 */
object SparkSqlFromMysqlDemo {

    def main(args: Array[String]): Unit = {
        Logger.getLogger("SparkSqlFromMysqlDemo").setLevel(Level.WARN)
        val spark: SparkSession = SparkSession
            .builder()
            .master("spark://localhost:7077")
            .appName("spark sql from mysql demo")
            .getOrCreate()

        val jdbcDF: DataFrame = spark
            .read
            .format("jdbc")
            .option("url", "jdbc:mysql://10.31.194.51:3306/data")
            .option("dbtable", "t_potential_customer_leads_m")
            .option("user", "root")
            .option("password", "root")
            .load()
            .persist(StorageLevel.MEMORY_AND_DISK)

        import spark.implicits._

        jdbcDF.createOrReplaceTempView("Leads")

        val sql = "select mobile,count(*) as cnt from Leads group by mobile"
        val sqlDF: DataFrame = spark.sql(sql)

//        sqlDF.show(100)

        sqlDF.write
            .format("jdbc")
            .option("url", "jdbc:mysql://10.31.194.51:3306/data?useUnicode=true&characterEncoding=utf8")
            .option("dbtable", "data.result1")
            .option("user", "root")
            .option("password", "root")
            .mode("overwrite")
            .save()

    }


    case class Leads(

                        id: String,
                        potential_customer_name: String,
                        order_id: String,
                        data_type: String,
                        order_type: String,
                        data_source_from_website: String,
                        coming_order_activity_time: String,
                        mobile: String,
                        contact_phone: String,
                        called_phone: String,
                        sina_weibo: String,
                        tencent_weibo: String,
                        weixin: String,
                        qq_number: String,
                        business_type_code: String,
                        first_resource_code: String,
                        first_resource_name: String,
                        second_resource_code: String,
                        second_resource_name: String,
                        dealer_id: String,
                        dealer_name: String,
                        dealer_account: String,
                        main_contact_type: String,
                        tel_duration: String,
                        address: String,
                        reg_date: String,
                        create_by: String,
                        create_org: String,
                        create_time: String,
                        leads_resource_type: String,
                        sales_id: String,
                        sales_name: String,
                        processing_status: String,
                        processing_result: String,
                        allocate_status: String,
                        check_status: String,
                        import_id: String,
                        relation_leads_id: String,
                        leads_status: String,
                        clean_time: String,
                        allocate_time: String,
                        cleaning_customer_status: String,
                        allocate_by: String,
                        review_by: String,
                        review_time: String,
                        feedback_level: String,
                        activity_id: String,
                        update_time: String)


}
