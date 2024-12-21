package com.liangheee.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.base.BaseApp;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.function.AsyncDimJoinMapFunction;
import com.liangheee.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.liangheee.gmall.realtime.common.function.DimJoinMapFunction;
import com.liangheee.gmall.realtime.common.utils.DateFormatUtil;
import com.liangheee.gmall.realtime.common.utils.FlinkSinkUtil;
import com.liangheee.gmall.realtime.common.utils.HBaseUtil;
import com.liangheee.gmall.realtime.common.utils.RedisUtil;
import com.liangheee.gmall.realtime.dws.bean.TradeSkuOrderBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * 交易域SKU粒度下单各窗口汇总表
 * 按照SKU维度分组，统计原始金额、活动减免金额、优惠券减免金额和订单金额
 * @author liangheee
 * * @date 2024-12-08
 */
public class DwsTradeSkuOrderWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeSkuOrderWindow().start(
                "10029",
                4,
                Constant.DORIS_DWS_TRADE_SKU_ORDER_WINDOW,
                Constant.BROKER_SERVERS,
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 过滤下单明细事实表中的空数据，并且转换为JsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                if (!StringUtils.isEmpty(jsonStr)) {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    out.collect(jsonObj);
                }
            }
        });

        // {
        //  "create_time": "2024-12-04 15:35:56",
        //  "sku_num": "1",
        //  "pt": "2024-12-08 07:35:56.968Z",
        //  "split_original_amount": "8197.0000",
        //  "uct": 1733643356,
        //  "split_coupon_amount": "0.0",
        //  "sku_id": "11",
        //  "date_id": "2024-12-04",
        //  "ct": "2024-12-08 07:35:56.968Z",
        //  "user_id": "1848",
        //  "province_id": "4",
        //  "sku_name": "Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机",
        //  "id": "34327",
        //  "order_id": "24599",
        //  "split_activity_amount": "0.0",
        //  "split_total_amount": "8197.0",
        //  "ts": 1733643356,
        //  "ut": 1733643356
        //}
//        jsonObjDS.print();

        // 处理事实表数据中的重复数据
        //      产生重复数据的原因：
        //          DWD订单明细事实表的组成  订单明细表 join 订单表 left join 订单活动表 left join 订单优惠卷表
        //          对于Flink SQL而言，使用左外连接，如果左表数据先到，右表数据后到，Flink流会产生3条数据：
        //              左表  null  +I
        //              左表  null  -D
        //              左表  右表   +I
        //          那么通过Upsert-Kafka-Connector Sink写入Kafka中也会产生3条数据：
        //              左表  null
        //              null
        //              左表  右表
        //      处理重复数据的两种方案：
        //          1、状态 + 定时器
        //          2、状态 + 抵消

        // 去重之前先按照订单明细Id分组，使得相同明细id的数据进入Flink同一个并行度
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));

        // 1、状态 + 定时器
        //      通过状态存储上一条到达的数据，并且设置一个定时服务
        //          如果出现重复数据，当下一条数据到达时获取状态中存储的数据中的处理时间，进行比对，更新状态存储数据为处理时间更大的数据
        //      定时服务触发执行，向下游传递状态中的数据，并且清除状态
//        SingleOutputStreamOperator<TradeSkuOrderBean> filterRepeatOrderDetailDS = orderDetailIdKeyedDS.process(new KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>() {
//
//            private ValueState<JSONObject> lastJsonObjSate;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<JSONObject>("last-jsonObj-state", JSONObject.class);
//                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(5))
//                        .updateTtlOnCreateAndWrite()
//                        .returnExpiredIfNotCleanedUp()
//                        .build());
//                lastJsonObjSate = getRuntimeContext().getState(valueStateDescriptor);
//            }
//
//            @Override
//            public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>.Context context, Collector<TradeSkuOrderBean> out) throws Exception {
//                JSONObject lastJsonObj = lastJsonObjSate.value();
//
//                if (lastJsonObj != null) {
//                    // 如果出现重复数据，比较处理时间，决定是否更新状态
//                    long lastProcessTs = lastJsonObj.getLongValue("pt") * 1000L;
//                    long processTs = jsonObj.getLongValue("pt") * 1000L;
//                    if (processTs > lastProcessTs) {
//                        lastJsonObjSate.update(jsonObj);
//                    }
//                } else {
//                    // 如果时第一条到来的数据，直接更新，并启动定时服务
//                    lastJsonObjSate.update(jsonObj);
//                    long currentProcessingTime = context.timerService().currentProcessingTime();
//                    context.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
//                }
//            }
//
//            @Override
//            public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>.OnTimerContext context, Collector<TradeSkuOrderBean> out) throws Exception {
//                JSONObject jsonObj;
//                if(lastJsonObjSate != null && (jsonObj = lastJsonObjSate.value()) != null){
//                    lastJsonObjSate.clear();
//                    TradeSkuOrderBean tradeSkuOrderBean = TradeSkuOrderBean.builder()
//                            .skuId(jsonObj.getString("sku_id"))
//                            .skuName(jsonObj.getString("sku_name"))
//                            .originalAmount(jsonObj.getBigDecimal("split_original_amount"))
//                            .activityReduceAmount(jsonObj.getBigDecimal("split_activity_amount"))
//                            .couponReduceAmount(jsonObj.getBigDecimal("split_coupon_amount"))
//                            .orderAmount(jsonObj.getBigDecimal("split_total_amount"))
//                            .ts(jsonObj.getLong("ts") * 1000L)
//                            .build();
//                    out.collect(tradeSkuOrderBean);
//                }
//            }
//        });

        // 2、状态 + 抵消
        //      第一条数据到来时，写入状态中存储，并且向下游传递
        //      第二条数据到来时，取出状态中存储的数据，将该数据中影响度量值的字段取反，向下游传递，同时将第二条数据向下游传递
        SingleOutputStreamOperator<TradeSkuOrderBean> filterRepeatOrderDetailDS = orderDetailIdKeyedDS.process(new KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>() {
            private ValueState<JSONObject> lastJsonObjSate;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<JSONObject>("last-jsonObj-state", JSONObject.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(5))
                        .updateTtlOnCreateAndWrite()
                        .returnExpiredIfNotCleanedUp()
                        .build());
                lastJsonObjSate = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>.Context ctx, Collector<TradeSkuOrderBean> out) throws Exception {
                JSONObject lastJsonObj = lastJsonObjSate.value();
                TradeSkuOrderBean tradeSkuOrderBean = TradeSkuOrderBean.builder()
                        .skuId(jsonObj.getString("sku_id"))
                        .skuName(jsonObj.getString("sku_name"))
                        .originalAmount(jsonObj.getBigDecimal("split_original_amount"))
                        .activityReduceAmount(jsonObj.getBigDecimal("split_activity_amount"))
                        .couponReduceAmount(jsonObj.getBigDecimal("split_coupon_amount"))
                        .orderAmount(jsonObj.getBigDecimal("split_total_amount"))
                        .ts(jsonObj.getLong("ts") * 1000L)
                        .build();
                lastJsonObjSate.update(jsonObj);
                out.collect(tradeSkuOrderBean);

                if (lastJsonObj != null) {
                    // 第二条数据到来时，取出状态中存储的数据，将该数据中影响度量值的字段取反，向下游传递，同时将第二条数据向下游传递
                    TradeSkuOrderBean lastTradeSkuOrderBean = TradeSkuOrderBean.builder()
                            .skuId(lastJsonObj.getString("sku_id"))
                            .skuName(lastJsonObj.getString("sku_name"))
                            .originalAmount(new BigDecimal("-" + lastJsonObj.getString("split_original_amount")))
                            .activityReduceAmount(new BigDecimal("-" + lastJsonObj.getString("split_activity_amount")))
                            .couponReduceAmount(new BigDecimal("-" + lastJsonObj.getString("split_coupon_amount")))
                            .orderAmount(new BigDecimal("-" + lastJsonObj.getString("split_total_amount")))
                            .ts(lastJsonObj.getLong("ts") * 1000L)
                            .build();
                    out.collect(lastTradeSkuOrderBean);
                    lastJsonObjSate.clear();
                }
            }
        });

//        filterRepeatOrderDetailDS.print();

        // 分配水位线和时间戳
        SingleOutputStreamOperator<TradeSkuOrderBean> tradeSkuOrderBeanWithWatermarkDS = filterRepeatOrderDetailDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TradeSkuOrderBean>forMonotonousTimestamps()
                        .withTimestampAssigner((tradeSkuOrderBean, recordTimestamp) -> tradeSkuOrderBean.getTs())
        );

        //  按照skuId分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS = tradeSkuOrderBeanWithWatermarkDS.keyBy(TradeSkuOrderBean::getSkuId);

        //  开窗
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowedDS = skuIdKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //  聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = windowedDS.reduce(new ReduceFunction<TradeSkuOrderBean>() {
            @Override
            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                return value1;
            }
        }, new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) throws Exception {
                TradeSkuOrderBean tradeSkuOrderBean = elements.iterator().next();
                TimeWindow window = context.window();
                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                String curDate = DateFormatUtil.tsToDate(window.getStart());
                tradeSkuOrderBean.setStt(stt);
                tradeSkuOrderBean.setEdt(edt);
                tradeSkuOrderBean.setCurDate(curDate);
                out.collect(tradeSkuOrderBean);
            }
        });

//        reduceDS.print();

        // 关联维度属性的方式
        //      1、基本的维度关联方式，每一次都去HBase中查询维度属性
        //      2、旁路缓存
        //      3、旁路缓存 + 异步IO进一步提升性能

        // 通过skuId关联维度属性
        //      1、最基本的维度关联方式，每一次都去HBase中查询维度属性
//        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoJoinDimDS = reduceDS.map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
//            private Connection hbaseConn;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                hbaseConn = HBaseUtil.getHBaseConnection();
//            }
//
//            @Override
//            public void close() throws Exception {
//                HBaseUtil.closeHBaseConnection(hbaseConn);
//            }
//
//            @Override
//            public TradeSkuOrderBean map(TradeSkuOrderBean tradeSkuOrderBean) {
//                String skuId = tradeSkuOrderBean.getSkuId();
//                JSONObject dimJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId, JSONObject.class, false);
//                if (dimJsonObj != null) {
//                    tradeSkuOrderBean.setSpuId(dimJsonObj.getString("spu_id"));
//                    tradeSkuOrderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
//                    tradeSkuOrderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
//                    return tradeSkuOrderBean;
//                } else {
//                    throw new RuntimeException("在HBase中通过skuId=" + skuId + "查询到关联维度信息不存在");
//                }
//            }
//        });

        //      2、旁路缓存
        //          缓存组件选型
        //              状态  时效性高，但是jvm进程之间不共享，多并行度下存储重复数据存在数据膨胀
        //              Redis 时效性较高，并且作为中间件可以共享给所有并行度jvm线程
//        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoJoinDimDS = reduceDS.map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
//            private Connection hbaseConn;
//            private Jedis redisConn;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                hbaseConn = HBaseUtil.getHBaseConnection();
//                redisConn = RedisUtil.getRedisConnection();
//            }
//
//            @Override
//            public void close() throws Exception {
//                HBaseUtil.closeHBaseConnection(hbaseConn);
//                RedisUtil.closeRedisConnection(redisConn);
//            }
//
//            @Override
//            public TradeSkuOrderBean map(TradeSkuOrderBean tradeSkuOrderBean) throws Exception {
//                // 获取维度外键
//                String skuId = tradeSkuOrderBean.getSkuId();
//                // 通过维度外键到缓存Redis中找数据是否存在
//                JSONObject dimJsonObj = RedisUtil.getDim(redisConn, "dim_sku_info", skuId);
//                //      如果存在，直接关联维度属性到流数据中
//                //      如果不存在，到HBase中获取维度数据，将该维度数据添加到Redis中，并且关联维度属性到流数据
//                if (dimJsonObj == null) {
//                    dimJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId, JSONObject.class, false);
//                    if (dimJsonObj != null) {
//                        RedisUtil.setDim(redisConn, "dim_sku_info", skuId, dimJsonObj);
//                    } else {
//                        throw new RuntimeException("在HBase中通过skuId=" + skuId + "查询到关联维度信息不存在");
//                    }
//                }
//
//                tradeSkuOrderBean.setSpuId(dimJsonObj.getString("spu_id"));
//                tradeSkuOrderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
//                tradeSkuOrderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
//                return tradeSkuOrderBean;
//            }
//        });
        //      2、旁路缓存 + 模板方法设计模式
//        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoJoinDimDS = reduceDS.map(new DimJoinMapFunction<TradeSkuOrderBean>() {
//            @Override
//            public String getDimKey(TradeSkuOrderBean tradeSkuOrderBean) {
//                return tradeSkuOrderBean.getSkuId();
//            }
//
//            @Override
//            public String getDimTable() {
//                return "dim_sku_info";
//            }
//
//            @Override
//            public void joinDim(TradeSkuOrderBean tradeSkuOrderBean, JSONObject dimJsonObj) {
//                tradeSkuOrderBean.setSpuId(dimJsonObj.getString("spu_id"));
//                tradeSkuOrderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
//                tradeSkuOrderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
//            }
//        });

        //      3、旁路缓存 + 异步IO
        //          背景：通过读取外部数据来补充流中的数据，是一个非常耗费性能的操作
        //          对于map算子而言，在同一个并行度中，里面的数据转换是一个串行的操作，此时想要提高整个Flink的处理能力，我们可以提高Flink程序的并行度，但是这就意味着高资源的消耗，需要补充较多的硬件资源，这是无穷无尽没有上限的，是不可能
        //          因此我们考虑在每一个并行度中，使用异步IO的方式读取外部数据，提高Flink单个并行度的并发能力，从而提高整个Flink程序的并发能力
//        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoJoinDimDS = AsyncDataStream.unorderedWait(
//                reduceDS,
//                new RichAsyncFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
//                    private Connection hbaseConn;
//                    private Jedis redisConn;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        hbaseConn = HBaseUtil.getHBaseConnection();
//                        redisConn = RedisUtil.getRedisConnection();
//                    }
//
//                    @Override
//                    public void close() throws Exception {
//                        HBaseUtil.closeHBaseConnection(hbaseConn);
//                        RedisUtil.closeRedisConnection(redisConn);
//                    }
//
//                    @Override
//                    public void asyncInvoke(TradeSkuOrderBean tradeSkuOrderBean, ResultFuture<TradeSkuOrderBean> resultFuture) throws Exception {
//                        // 获取维度外键
//                        String skuId = tradeSkuOrderBean.getSkuId();
//                        // 通过维度外键到缓存Redis中找数据是否存在
//                        JSONObject dimJsonObj = RedisUtil.getDim(redisConn, "dim_sku_info", skuId);
//                        //      如果存在，直接关联维度属性到流数据中
//                        //      如果不存在，到HBase中获取维度数据，将该维度数据添加到Redis中，并且关联维度属性到流数据
//                        if (dimJsonObj == null) {
//                            dimJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId, JSONObject.class, false);
//                            if (dimJsonObj != null) {
//                                RedisUtil.setDim(redisConn, "dim_sku_info", skuId, dimJsonObj);
//                            } else {
//                                throw new RuntimeException("在HBase中通过skuId=" + skuId + "查询到关联维度信息不存在");
//                            }
//                        }
//
//                        tradeSkuOrderBean.setSpuId(dimJsonObj.getString("spu_id"));
//                        tradeSkuOrderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
//                        tradeSkuOrderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
//
//                        resultFuture.complete(Collections.singleton(tradeSkuOrderBean));
//                    }
//                },
//                10,
//                TimeUnit.SECONDS
//        );

        //      3、旁路缓存 + 异步IO + 模板方法设计模式
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoJoinDimDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new AsyncDimJoinMapFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getDimKey(TradeSkuOrderBean tradeSkuOrderBean) {
                        return tradeSkuOrderBean.getSkuId();
                    }

                    @Override
                    public String getDimTable() {
                        return "dim_sku_info";
                    }

                    @Override
                    public void joinDim(TradeSkuOrderBean tradeSkuOrderBean, JSONObject dimJsonObj) {
                        tradeSkuOrderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        tradeSkuOrderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                        tradeSkuOrderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                    }
                },
                10,
                TimeUnit.SECONDS
        );

//        withSkuInfoJoinDimDS.print();

        // 通过spuId关联维度属性
        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = AsyncDataStream.unorderedWait(
                withSkuInfoJoinDimDS,
                new AsyncDimJoinMapFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getDimKey(TradeSkuOrderBean tradeSkuOrderBean) {
                        return tradeSkuOrderBean.getSpuId();
                    }

                    @Override
                    public String getDimTable() {
                        return "dim_spu_info";
                    }

                    @Override
                    public void joinDim(TradeSkuOrderBean tradeSkuOrderBean, JSONObject dimJsonObj) {
                        tradeSkuOrderBean.setSpuName(dimJsonObj.getString("spu_name"));
                    }
                },
                10,
                TimeUnit.SECONDS
        );


        // 通过tmId关联维度属性
        SingleOutputStreamOperator<TradeSkuOrderBean> withTrademarkInfoDS = AsyncDataStream.unorderedWait(
                withSpuInfoDS,
                new AsyncDimJoinMapFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getDimKey(TradeSkuOrderBean tradeSkuOrderBean) {
                        return tradeSkuOrderBean.getTrademarkId();
                    }

                    @Override
                    public String getDimTable() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public void joinDim(TradeSkuOrderBean tradeSkuOrderBean, JSONObject dimJsonObj) {
                        tradeSkuOrderBean.setTrademarkName(dimJsonObj.getString("tm_name"));
                    }
                },
                10,
                TimeUnit.SECONDS
        );

        // 通过category3关联维度属性
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory3InfoDS = AsyncDataStream.unorderedWait(
                withTrademarkInfoDS,
                new AsyncDimJoinMapFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getDimKey(TradeSkuOrderBean tradeSkuOrderBean) {
                        return tradeSkuOrderBean.getCategory3Id();
                    }

                    @Override
                    public String getDimTable() {
                        return "dim_base_category3";
                    }

                    @Override
                    public void joinDim(TradeSkuOrderBean tradeSkuOrderBean, JSONObject dimJsonObj) {
                        tradeSkuOrderBean.setCategory3Name(dimJsonObj.getString("name"));
                        tradeSkuOrderBean.setCategory2Id(dimJsonObj.getString("category2_id"));
                    }
                },
                10,
                TimeUnit.SECONDS
        );

        // 通过category2关联维度属性
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory2InfoDS = AsyncDataStream.unorderedWait(
                withCategory3InfoDS,
                new AsyncDimJoinMapFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getDimKey(TradeSkuOrderBean tradeSkuOrderBean) {
                        return tradeSkuOrderBean.getCategory2Id();
                    }

                    @Override
                    public String getDimTable() {
                        return "dim_base_category2";
                    }

                    @Override
                    public void joinDim(TradeSkuOrderBean tradeSkuOrderBean, JSONObject dimJsonObj) {
                        tradeSkuOrderBean.setCategory2Name(dimJsonObj.getString("name"));
                        tradeSkuOrderBean.setCategory1Id(dimJsonObj.getString("category1_id"));
                    }
                },
                10,
                TimeUnit.SECONDS
        );

        // 通过category1关联维度属性
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory1InfoDS = AsyncDataStream.unorderedWait(
                withCategory2InfoDS,
                new AsyncDimJoinMapFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getDimKey(TradeSkuOrderBean tradeSkuOrderBean) {
                        return tradeSkuOrderBean.getCategory1Id();
                    }

                    @Override
                    public String getDimTable() {
                        return "dim_base_category1";
                    }

                    @Override
                    public void joinDim(TradeSkuOrderBean tradeSkuOrderBean, JSONObject dimJsonObj) {
                        tradeSkuOrderBean.setCategory1Name(dimJsonObj.getString("name"));
                    }
                },
                10,
                TimeUnit.SECONDS
        );

//        withCategory1InfoDS.print();

        //  写入Doris
        withCategory1InfoDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_FE_NODES,
                        Constant.DORIS_USER,
                        Constant.DORIS_PASSWD,
                        Constant.DORIS_DATABASE,
                        Constant.DORIS_DWS_TRADE_SKU_ORDER_WINDOW));
    }
}
