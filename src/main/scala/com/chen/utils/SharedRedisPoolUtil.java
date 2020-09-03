package com.chen.utils;

import redis.clients.jedis.ShardedJedis;

/**
 * @author bystander
 * @date 2018/12/22
 */
public class SharedRedisPoolUtil {


    public static String set(String key, String value) {
        ShardedJedis jedis = null;
        String result = null;

        try {
            jedis = JedisShardedPool.getJedis();
            result = jedis.set(key, value);
        } catch (Exception e) {
            jedis.close();
        }
        jedis.close();
        return result;


    }
}
