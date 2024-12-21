package com.liangheee.gmall.realtime.common.function;


import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.template.DimJoinFunction;
import com.liangheee.gmall.realtime.common.utils.HBaseUtil;
import com.liangheee.gmall.realtime.common.utils.RedisUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

/**
 * @author liangheee
 * * @date 2024-12-08
 */
public abstract class DimJoinMapFunction<T> extends RichMapFunction<T, T> implements DimJoinFunction<T> {
    private Connection hbaseConn;
    private Jedis redisConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HBaseUtil.getHBaseConnection();
        redisConn = RedisUtil.getRedisConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(hbaseConn);
        RedisUtil.closeRedisConnection(redisConn);
    }

    @Override
    public T map(T obj) throws Exception {
        // 获取维度外键
        String dimKey = getDimKey(obj);

        // 获取维度表表名
        String tableName = getDimTable();

        // 通过维度外键到缓存Redis中找数据是否存在
        JSONObject dimJsonObj = RedisUtil.getDim(redisConn, tableName, dimKey);
        //      如果存在，直接关联维度属性到流数据中
        //      如果不存在，到HBase中获取维度数据，将该维度数据添加到Redis中，并且关联维度属性到流数据
        if (dimJsonObj == null) {
            dimJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, tableName, dimKey, JSONObject.class, false);
            if (dimJsonObj != null) {
                RedisUtil.setDim(redisConn, tableName, dimKey, dimJsonObj);
            } else {
                throw new RuntimeException("在HBase中查询tableName=" + tableName + "的维度主键值为：" + dimKey + "的维度信息不存在");
            }
        }

        joinDim(obj,dimJsonObj);

        return obj;
    }


}
