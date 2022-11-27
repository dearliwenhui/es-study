package com.ab.core.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

/**
 * @description:
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2022-11-26 22:19
 **/
public class RedisUtil {

    private static JedisPool jedisPool = null;

    static {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(10);
        poolConfig.setMaxTotal(100);
        poolConfig.setMaxWait(Duration.ofMillis(2000));
        poolConfig.setTestOnBorrow(true);

        jedisPool = new JedisPool(poolConfig, "myhost", 6379, null, "redis");
    }

    private RedisUtil() {
    }

    public static Jedis getJedis() {
        return jedisPool.getResource();
    }

    //向连接池归还连接
    public static void returnResource(Jedis jedis) {
        jedis.close();
    }

}
