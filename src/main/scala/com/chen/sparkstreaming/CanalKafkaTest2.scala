package com.chen.sparkstreaming

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * @author bystander
 * @date 2020/4/2
 */
object CanalKafkaTest2 {

    def main(args: Array[String]): Unit = {
        // 设置日志的输出级别
        Logger.getLogger("org").setLevel(Level.ERROR)

        val conf = new SparkConf()
            .setMaster(PropertiesUtil.getPropString("spark.master"))
            .setAppName(PropertiesUtil.getPropString("spark.app.name"))
            // ！！必须设置，否则Kafka数据会报无法序列化的错误
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        //如果环境中已经配置HADOOP_HOME则可以不用设置hadoop.home.dir
        // System.setProperty("hadoop.home.dir", "/Users/yoreyuan/soft/hadoop-2.9.2")

        val ssc = new StreamingContext(conf, Seconds(PropertiesUtil.getPropInt("spark.streaming.durations.sec").toLong))
        ssc.sparkContext.setLogLevel("ERROR")
        ssc.checkpoint(PropertiesUtil.getPropString("spark.checkout.dir"))

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> PropertiesUtil.getPropString("bootstrap.servers"),
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> PropertiesUtil.getPropString("group.id"),
            "auto.offset.reset" -> PropertiesUtil.getPropString("auto.offset.reset"),
            "enable.auto.commit" -> (PropertiesUtil.getPropBoolean("enable.auto.commit"): java.lang.Boolean)
        )
        val topics = Array(PropertiesUtil.getPropString("kafka.topic.name"))

        val kafkaStreaming = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )


        kafkaStreaming.map[JSONObject](line => { // str转成JSONObject
            println("$$$\t" + line.value())
            JSON.parseObject(line.value)
        }).flatMap[(JSONObject, JSONObject)](jsonObj => { // 将改变前和改变后的数据转成Tuple
            var oldJsonArr: JSONArray = jsonObj.getJSONArray("old")
            val dataJsonArr: JSONArray = jsonObj.getJSONArray("data")
            if ("INSERT".equals(jsonObj.getString("type"))) {
                oldJsonArr = new JSONArray()
                val oldJsonObj2 = new JSONObject()
                oldJsonObj2.put("mor_rate", "0")
                oldJsonArr.add(oldJsonObj2)
            }

            val result = ListBuffer[(JSONObject, JSONObject)]()

            for (i <- 0 until oldJsonArr.size) {
                val jsonTuple = (oldJsonArr.getJSONObject(i), dataJsonArr.getJSONObject(i))
                result += jsonTuple
            }
            result
        }).map(t => {
            val id = t._2.getString("id").toLong
            val username = t._2.getString("username")
            val password = t._2.getString("password")
            val phone = t._2.getString("phone")
            val created = t._2.getString("created")
            val salt = t._2.getString("salt")

            (id, username, password, phone, created, salt)
        }).foreachRDD(rdd => {
            rdd.foreachPartition(p => {
                val paramsList = ListBuffer[ParamsList]()
                val jdbcWrapper = JDBCWrapper.getInstance()
                while (p.hasNext) {
                    val record = p.next()
                    val paramsListTmp = new ParamsList
                    paramsListTmp.id = record._1
                    paramsListTmp.username = record._2
                    paramsListTmp.password = record._3
                    paramsListTmp.phone = record._4
                    paramsListTmp.created = record._5
                    paramsListTmp.salt = record._6
                    paramsListTmp.params_Type = "real_risk"
                    paramsList += paramsListTmp
                }
                /**
                 * VALUES(p_num, risk_rank, mor_rate, ch_mor_rate, load_time)
                 */
                val insertNum = jdbcWrapper.doBatch("INSERT INTO tb_user2 VALUES(?,?,?,?,?,?)", paramsList)
                println("INSERT TABLE real_risk: " + insertNum.mkString(", "))
            })
        })

        ssc.start()
        ssc.awaitTermination()


    }

}

/**
 * 结果表对应的参数实体对象
 */
class ParamsList extends Serializable {
    var id: Long = _
    var username: String = _
    var password: String = _
    var phone: String = _
    var created: String = _
    var salt: String = _
    var params_Type: String = _

    override def toString = s"ParamsList($id, $username, $password, $phone, $created, $salt)"
}

