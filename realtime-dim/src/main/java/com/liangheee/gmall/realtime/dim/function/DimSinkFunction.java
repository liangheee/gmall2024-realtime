package com.liangheee.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.bean.TableProcessDim;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.utils.HBaseUtil;
import com.liangheee.gmall.realtime.common.utils.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

/**
 * @author liangheee
 * * @date 2024/10/30
 */
public class DimSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
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
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
        JSONObject jsonObj = value.f0;
        TableProcessDim tableProcessDim = value.f1;

        String type = jsonObj.getString("type");
        jsonObj.remove("type");
        String sinkTable = tableProcessDim.getSinkTable();
        String sinkRowKeyColumn = tableProcessDim.getSinkRowKey();
        String rowKey = jsonObj.getString(sinkRowKeyColumn);
        if("delete".equals(type)){
            // 如果是delete，对HBase数据行进行删除操作
            HBaseUtil.deleteRow(hbaseConn, Constant.HBASE_NAMESPACE,sinkTable,rowKey);
        }else{
            // 如果是insert、update、bootstrap-insert，对HBase进行put操作
            String sinkFamily = tableProcessDim.getSinkFamily();
            HBaseUtil.putRow(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamily,rowKey,jsonObj);
        }

        // 维度发生变换的时候，删除缓存redis中缓存数据
        if("delete".equals(type) || "update".equals(type)){
            // 如果是delete，删除Redis中缓存的维度数据
            RedisUtil.delDim(redisConn,sinkTable,rowKey);
        }
    }
}
