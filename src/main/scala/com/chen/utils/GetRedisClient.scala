//package com.chen.utils
//
//import redis.clients.jedis.Jedis
//
///**
// * @author bystander
// * @date 2020/4/24
// */
//object GetRedisClient {
//
//    def getRedisClient(redisUgi:String):Jedis = {
//        val port = -1
//        val ip = "localhost"
//        val (redisHost, redisPort) = getConfig()
//
//        val redisClient = new Jedis(ip, port)
//
//        redisClient.auth(redisUgi)
//        redisClient
//    }
//
//}
