package com.liangheee.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.base.BaseApp;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.function.AsyncDimJoinMapFunction;
import com.liangheee.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.liangheee.gmall.realtime.common.utils.DateFormatUtil;
import com.liangheee.gmall.realtime.common.utils.FlinkSinkUtil;
import com.liangheee.gmall.realtime.dws.bean.TradeProvinceOrderBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

/**
 * 交易域省份粒度下单各窗口汇总表
 * 统计各省份各窗口订单数和订单金额，将数据写入Doris交易域省份粒度下单各窗口汇总表
 * @author liangheee
 * * @date 2024-12-09
 */
public class DwsTradeProvinceOrderWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeProvinceOrderWindow().start(
                "10030",
                4,
                Constant.DORIS_DWS_TRADE_PROVINCE_ORDER_WINDOW,
                Constant.BROKER_SERVERS,
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
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
        SingleOutputStreamOperator<TradeProvinceOrderBean> tradeProvinceOrderBeanDS = kafkaStrDS.process(new ProcessFunction<String, TradeProvinceOrderBean>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, TradeProvinceOrderBean>.Context ctx, Collector<TradeProvinceOrderBean> out) throws Exception {
                if (!StringUtils.isEmpty(jsonStr)) {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    TradeProvinceOrderBean tradeProvinceOrderBean = TradeProvinceOrderBean.builder()
                            .orderId(jsonObj.getString("order_id"))
                            .orderDetailId(jsonObj.getString("id"))
                            .provinceId(jsonObj.getString("province_id"))
                            .orderAmount(jsonObj.getBigDecimal("split_total_amount"))
                            .ts(jsonObj.getLong("ts") * 1000L)
                            .build();
                    out.collect(tradeProvinceOrderBean);
                }
            }
        });

        KeyedStream<TradeProvinceOrderBean, String> orderDetailIdKeyedDS = tradeProvinceOrderBeanDS.keyBy(TradeProvinceOrderBean::getOrderDetailId);

        SingleOutputStreamOperator<TradeProvinceOrderBean> filterRepeatOrderDetailDS = orderDetailIdKeyedDS.process(new KeyedProcessFunction<String, TradeProvinceOrderBean, TradeProvinceOrderBean>() {
            private ValueState<TradeProvinceOrderBean> lastTradeProvinceOrderState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<TradeProvinceOrderBean> valueStateDescriptor = new ValueStateDescriptor<TradeProvinceOrderBean>("last-trade-province-order-state", TradeProvinceOrderBean.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(5))
                        .updateTtlOnCreateAndWrite()
                        .returnExpiredIfNotCleanedUp()
                        .build());
                lastTradeProvinceOrderState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(TradeProvinceOrderBean tradeProvinceOrderBean, KeyedProcessFunction<String, TradeProvinceOrderBean, TradeProvinceOrderBean>.Context ctx, Collector<TradeProvinceOrderBean> out) throws Exception {
                TradeProvinceOrderBean lastTradeProvinceOrderBean = lastTradeProvinceOrderState.value();
                lastTradeProvinceOrderState.update(tradeProvinceOrderBean);
                out.collect(tradeProvinceOrderBean);

                if (lastTradeProvinceOrderBean != null) {
                    lastTradeProvinceOrderBean.setOrderAmount(new BigDecimal("-1").multiply(lastTradeProvinceOrderBean.getOrderAmount()));
                    lastTradeProvinceOrderState.clear();
                    out.collect(lastTradeProvinceOrderBean);
                }
            }
        });

//        filterRepeatOrderDetailDS.print();

        KeyedStream<TradeProvinceOrderBean, String> orderIdKeyedDS = filterRepeatOrderDetailDS.keyBy(TradeProvinceOrderBean::getOrderId);

        SingleOutputStreamOperator<TradeProvinceOrderBean> filterRepeatOrderIdDS = orderIdKeyedDS.process(new KeyedProcessFunction<String, TradeProvinceOrderBean, TradeProvinceOrderBean>() {
            private ValueState<Boolean> isAccessedState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Boolean> valueStateDescriptor = new ValueStateDescriptor<>("is-accessed-state", Types.BOOLEAN);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10))
                        .updateTtlOnCreateAndWrite()
                        .returnExpiredIfNotCleanedUp()
                        .build());
                isAccessedState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(TradeProvinceOrderBean tradeProvinceOrderBean, KeyedProcessFunction<String, TradeProvinceOrderBean, TradeProvinceOrderBean>.Context ctx, Collector<TradeProvinceOrderBean> out) throws Exception {
                Boolean isAccessed = isAccessedState.value();
                if (isAccessed != null && isAccessed) {
                    tradeProvinceOrderBean.setOrderCount(0L);
                } else {
                    tradeProvinceOrderBean.setOrderCount(1L);
                    isAccessedState.update(true);
                }
                out.collect(tradeProvinceOrderBean);
            }
        });

//        filterRepeatOrderIdDS.print();

        SingleOutputStreamOperator<TradeProvinceOrderBean> tradeProvinceOrderWithWatermarkDS = filterRepeatOrderIdDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TradeProvinceOrderBean>forMonotonousTimestamps()
                        .withTimestampAssigner((tradeProvinceOrderBean, recordTimestamp) -> tradeProvinceOrderBean.getTs()
                        )
        );

        KeyedStream<TradeProvinceOrderBean, String> provinceIdKeyedDS = tradeProvinceOrderWithWatermarkDS.keyBy(TradeProvinceOrderBean::getProvinceId);

        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowedDS = provinceIdKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceDS = windowedDS.reduce(new ReduceFunction<TradeProvinceOrderBean>() {
            @Override
            public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                return value1;
            }
        }, new ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>.Context context, Iterable<TradeProvinceOrderBean> elements, Collector<TradeProvinceOrderBean> out) throws Exception {
                TradeProvinceOrderBean tradeProvinceOrderBean = elements.iterator().next();
                TimeWindow window = context.window();
                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                String curDate = DateFormatUtil.tsToDateTime(window.getStart());
                tradeProvinceOrderBean.setStt(stt);
                tradeProvinceOrderBean.setEdt(edt);
                tradeProvinceOrderBean.setCurDate(curDate);
                out.collect(tradeProvinceOrderBean);
            }
        });

//        reduceDS.print();

        SingleOutputStreamOperator<TradeProvinceOrderBean> withProvinceNameDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new AsyncDimJoinMapFunction<TradeProvinceOrderBean>() {
                    @Override
                    public String getDimKey(TradeProvinceOrderBean tradeProvinceOrderBean) {
                        return tradeProvinceOrderBean.getProvinceId();
                    }

                    @Override
                    public String getDimTable() {
                        return "dim_base_province";
                    }

                    @Override
                    public void joinDim(TradeProvinceOrderBean tradeProvinceOrderBean, JSONObject dimJsonObj) {
                        tradeProvinceOrderBean.setProvinceName(dimJsonObj.getString("name"));
                    }
                },
                10,
                TimeUnit.SECONDS
        );

//        withProvinceNameDS.print();

        withProvinceNameDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_FE_NODES,
                        Constant.DORIS_USER,
                        Constant.DORIS_PASSWD,
                        Constant.DORIS_DATABASE,
                        Constant.DORIS_DWS_TRADE_PROVINCE_ORDER_WINDOW));
    }
}
