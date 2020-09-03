package com.chen.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author bystander
 * @date 2018/12/22
 */
public class JedisPoolUtils {

    private static JedisPool pool;

    private static void init() {
        JedisPoolConfig config = new JedisPoolConfig();

        //最大连接数
        config.setMaxTotal(20);
        //最大空闲数
        config.setMaxIdle(10);
        //最小空闲数
        config.setMinIdle(2);

        //从jedis连接池中获取连接时，检验并返回可用的链接
        config.setTestOnBorrow(true);
        //把连接放回连接池时，检验并返回可用的链接
        config.setTestOnReturn(true);

        config.setBlockWhenExhausted(true);//连接耗尽的时候，是否阻塞，false会抛出异常，true阻塞直到超时。默认为true。

        pool = new JedisPool(config,"127.0.0.1",6379,1000*2);
    }

    static {
        init();
    }

    public static Jedis getJedis() {
        return pool.getResource();
    }


}
