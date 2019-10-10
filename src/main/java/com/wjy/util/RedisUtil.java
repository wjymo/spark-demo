package com.wjy.util;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;

public class RedisUtil {
    private static JedisPool jedisPool = null;

    private static final String HOST = "wang-108";
    private static final int PORT = 6379;

    public static synchronized Jedis getJedis(){

        if(null == jedisPool) {
            GenericObjectPoolConfig config = new JedisPoolConfig();
//            config.setMaxIdle(10);
//            config.setMaxTotal(100);
//            config.setMaxWaitMillis(1000);
//            config.setTestOnBorrow(true);

            jedisPool = new JedisPool(config, HOST, PORT,3000,"gggyvw");
        }

        return jedisPool.getResource();
    }

    public static Map<String, String> hgetAll(String key){
        Map<String, String> map=null;
        Jedis jedis=null;
        try {
            jedis= getJedis();
            map= jedis.hgetAll(key);
        } finally {
            if(jedis!=null){
                jedis.close();
            }
        }
        return map;
    }

    public static void hset(String key,String field,String value){
        Jedis jedis=null;
        try {
            jedis= getJedis();
            jedis.hset(key,field,value);
        } finally {
            if(jedis!=null){
                jedis.close();
            }
        }
    }

}
