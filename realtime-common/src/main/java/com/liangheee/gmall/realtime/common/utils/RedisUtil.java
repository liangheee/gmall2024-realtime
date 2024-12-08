package com.liangheee.gmall.realtime.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.constant.Constant;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.concurrent.ExecutionException;

/**
 * @author liangheee
 * * @date 2024-12-07
 */
@Slf4j
public class RedisUtil {

    private static JedisPool jedisPool;

    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMinIdle(5);
        jedisPoolConfig.setMaxIdle(100);
        jedisPoolConfig.setMaxTotal(5);
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPoolConfig.setMaxWaitMillis(2000);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(jedisPoolConfig, Constant.REDIS_HOST,Constant.REDIS_PORT,10000);
    }

    public static Jedis getRedisConnection(){
        Jedis jedis = jedisPool.getResource();
        log.info("获取Redis连接成功");
        return jedis;
    }

    public static void closeRedisConnection(Jedis conn){
        if(conn != null && conn.isConnected()){
            conn.close();
            log.info("关闭Redis连接成功");
        }
    }

    public static StatefulRedisConnection<String,String> getAsyncRedisConnection(){
        RedisClient redisClient = RedisClient.create("redis://" + Constant.REDIS_HOST + ":" + Constant.REDIS_PORT + "/0");
        log.info("获取Redis异步连接成功");
        return redisClient.connect();
    }

    public static void closeAsyncRedisConnection(StatefulRedisConnection<String,String> asyncConn){
        if(asyncConn != null && asyncConn.isOpen()){
            asyncConn.close();
            log.info("关闭Redis异步连接成功");
        }
    }

    public static JSONObject getDim(Jedis conn,String dimTable,String id){
        log.info("从redis中获取缓存数据");
        String jsonStr = conn.get(generateKey(dimTable, id));
        if(!StringUtils.isEmpty(jsonStr)){
            return JSONObject.parseObject(jsonStr);
        }
        return null;
    }

    public static void setDim(Jedis conn,String dimTable,String id,JSONObject dimJsonObj){
        log.info("向redis中缓存维度数据");
        conn.setex(generateKey(dimTable, id),24 * 60 * 60,dimJsonObj.toJSONString());
    }

    public static JSONObject getDimAsync(StatefulRedisConnection<String,String> conn,String dimTable,String id){
        log.info("从redis中异步获取缓存数据");
        RedisAsyncCommands<String, String> redisAsyncCommands = conn.async();
        try {
            String dimJsonStr = redisAsyncCommands.get(generateKey(dimTable, id)).get();
            return JSON.parseObject(dimJsonStr);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void setDimAsync(StatefulRedisConnection<String,String> conn,String dimTable,String id,JSONObject dimJsonObj){
        log.info("向redis中缓存维度数据");
        RedisAsyncCommands<String, String> redisAsyncCommands = conn.async();
        redisAsyncCommands.setex(generateKey(dimTable, id),24 * 60 * 60,dimJsonObj.toJSONString());
    }

    public static String generateKey(String dimTable,String id){
        return dimTable + ":" + id;
    }
}
