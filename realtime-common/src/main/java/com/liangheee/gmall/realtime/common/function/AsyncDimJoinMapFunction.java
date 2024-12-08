package com.liangheee.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.template.DimJoinFunction;
import com.liangheee.gmall.realtime.common.utils.HBaseUtil;
import com.liangheee.gmall.realtime.common.utils.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author liangheee
 * * @date 2024-12-09
 */
public abstract class AsyncDimJoinMapFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {
    private AsyncConnection hbaseConn;
    private StatefulRedisConnection<String,String> redisConn;

    private static final ThreadPoolExecutor executor;

    static {
        executor = new ThreadPoolExecutor(
                5,
                100,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(10000),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HBaseUtil.getAsyncHBaseConnection();
        redisConn = RedisUtil.getAsyncRedisConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeAsyncHBaseConnection(hbaseConn);
        RedisUtil.closeAsyncRedisConnection(redisConn);
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        // 使用异步编排，采用线程池线程去处理串行任务，隔离主线程
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                return getDimKey(obj);
            }
        },executor).thenApplyAsync(new Function<String, JSONObject>() {
            @Override
            public JSONObject apply(String dimKey) {
                String dimTable = getDimTable();
                // 通过维度外键到缓存Redis中找数据是否存在
                JSONObject dimJsonObj = RedisUtil.getDimAsync(redisConn, dimTable, dimKey);
                //      如果存在，直接关联维度属性到流数据中
                //      如果不存在，到HBase中获取维度数据，将该维度数据添加到Redis中，并且关联维度属性到流数据
                if (dimJsonObj == null) {
                    dimJsonObj = HBaseUtil.getRowAsync(hbaseConn, Constant.HBASE_NAMESPACE, dimTable, dimKey, JSONObject.class, false);
                    if (dimJsonObj != null) {
                        RedisUtil.setDimAsync(redisConn, dimTable, dimKey, dimJsonObj);
                    } else {
                        throw new RuntimeException("在HBase中查询tableName=" + dimTable + "的维度主键值为：" + dimKey + "的维度信息不存在");
                    }
                }
                return dimJsonObj;
            }
        }, executor).thenAcceptAsync(new Consumer<JSONObject>() {
            @Override
            public void accept(JSONObject dimJsonObj) {
                // 关联维度属性
                if(dimJsonObj != null){
                    joinDim(obj,dimJsonObj);
                    resultFuture.complete(Collections.singleton(obj));
                }
            }
        },executor);
    }
}
