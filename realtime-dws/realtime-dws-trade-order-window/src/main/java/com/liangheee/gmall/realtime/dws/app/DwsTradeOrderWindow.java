package com.liangheee.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.base.BaseApp;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.liangheee.gmall.realtime.common.utils.DateFormatUtil;
import com.liangheee.gmall.realtime.common.utils.FlinkSinkUtil;
import com.liangheee.gmall.realtime.dws.bean.TradeOrderBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 交易域下单各窗口汇总表
 * 从 Kafka订单明细主题读取数据，统计当日下单独立用户数和首次下单用户数，封装为实体类，写入Doris
 * @author liangheee
 * * @date 2024/12/4
 */
public class DwsTradeOrderWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeOrderWindow().start(
                "10028",
                4,
                Constant.DORIS_DWS_TRADE_ORDER_WINDOW,
                Constant.BROKER_SERVERS,
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> jsonObjWithWatermarksDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner((jsonObj, recordTimestamp) -> jsonObj.getLong("ts") * 1000L)
        );

//        jsonObjWithWatermarksDS.print();

        KeyedStream<JSONObject, String> userIdKeyedDS = jsonObjWithWatermarksDS.keyBy(jsonObj -> jsonObj.getString("user_id"));

        SingleOutputStreamOperator<TradeOrderBean> processedDS = userIdKeyedDS.process(new KeyedProcessFunction<String, JSONObject, TradeOrderBean>() {
            private ValueState<String> lastOrderDetailDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("last-order-detail-date-state", Types.STRING);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1))
                        .neverReturnExpired()
                        .updateTtlOnCreateAndWrite()
                        .build());
                lastOrderDetailDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, TradeOrderBean>.Context ctx, Collector<TradeOrderBean> out) throws Exception {
                Long ts = jsonObj.getLong("ts") * 1000L;
                String curDate = DateFormatUtil.tsToDate(ts);
                String lastOrderDetailDate = lastOrderDetailDateState.value();
                lastOrderDetailDateState.update(curDate);

                Long orderUniqueUserCount = 0L, orderNewUserCount = 0L;
                if (!curDate.equals(lastOrderDetailDate)) {
                    // 当日首次下单，标记首次下单用户
                    orderNewUserCount = 1L;

                    if (StringUtils.isEmpty(lastOrderDetailDate)) {
                        // 之前没有下单过，标记当日独立用户
                        orderUniqueUserCount = 1L;
                    }
                }

                if (orderNewUserCount != 0L || orderUniqueUserCount != 0L) {
                    TradeOrderBean tradeOrderBean = TradeOrderBean.builder()
                            .orderNewUserCount(orderNewUserCount)
                            .orderUniqueUserCount(orderUniqueUserCount)
                            .ts(ts)
                            .build();
                    out.collect(tradeOrderBean);
                }
            }
        });

//        processedDS.print();

        AllWindowedStream<TradeOrderBean, TimeWindow> windowedDS = processedDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        SingleOutputStreamOperator<TradeOrderBean> resultDS = windowedDS.reduce(new ReduceFunction<TradeOrderBean>() {
            @Override
            public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) throws Exception {
                value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                return value1;
            }
        }, new ProcessAllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>.Context context, Iterable<TradeOrderBean> elements, Collector<TradeOrderBean> out) throws Exception {
                TradeOrderBean tradeOrderBean = elements.iterator().next();
                TimeWindow window = context.window();
                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                String curDate = DateFormatUtil.tsToDate(window.getStart());
                tradeOrderBean.setStt(stt);
                tradeOrderBean.setEdt(edt);
                tradeOrderBean.setCurDate(curDate);
                out.collect(tradeOrderBean);
            }
        });

//        resultDS.print();

        resultDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_FE_NODES,
                        Constant.DORIS_USER,
                        Constant.DORIS_PASSWD,
                        Constant.DORIS_DATABASE,
                        Constant.DORIS_DWS_TRADE_ORDER_WINDOW));
    }
}
