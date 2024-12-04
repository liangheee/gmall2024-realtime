package com.liangheee.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.base.BaseApp;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.liangheee.gmall.realtime.common.utils.DateFormatUtil;
import com.liangheee.gmall.realtime.common.utils.FlinkSinkUtil;
import com.liangheee.gmall.realtime.dws.bean.TradePaymentBean;
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
 * 交易域支付成功各窗口汇总表
 * 从Kafka读取交易域支付成功主题数据，统计支付成功独立用户数和首次支付成功用户数
 * 启动进程：dwd订单明细、dwd订单支付成功、当前app
 * @author liangheee
 * * @date 2024/12/4
 */
public class DwsTradePaymentSucWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradePaymentSucWindow().start(
                "10027",
                4,
                Constant.DORIS_DWS_TRADE_PAYMENT_SUC_WINDOW,
                Constant.BROKER_SERVERS,
                Constant.TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 将订单支付成功事实表数据转化为通用对象JsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // 分配水位线和时间戳
        SingleOutputStreamOperator<JSONObject> jsonObjWithWatermarksDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner((jsonObj, recordTimestamp) -> jsonObj.getLong("ts") * 1000L)
        );
//        jsonObjWithWatermarksDS.print();

        // 按照user_id分组
        KeyedStream<JSONObject, String> userIdKeyedDS = jsonObjWithWatermarksDS.keyBy(jsonObj -> jsonObj.getString("user_id"));

        // 利用Flink状态编程，判断获取支付成功独立用户和首次支付成功用户
        SingleOutputStreamOperator<TradePaymentBean> processedDS = userIdKeyedDS.process(new KeyedProcessFunction<String, JSONObject, TradePaymentBean>() {

            private ValueState<String> lastOrderPaySucDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("last-order-pay-suc-date-state", Types.STRING);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1))
                        .neverReturnExpired()
                        .updateTtlOnCreateAndWrite()
                        .build());
                lastOrderPaySucDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, TradePaymentBean>.Context context, Collector<TradePaymentBean> out) throws Exception {
                Long ts = jsonObj.getLong("ts") * 1000L;
                String curDate = DateFormatUtil.tsToDate(ts);
                String lastOrderPaySucDate = lastOrderPaySucDateState.value();
                lastOrderPaySucDateState.update(curDate);

                Long paymentSucUniqueUserCount = 0L,paymentSucNewUserCount = 0L;
                if(!curDate.equals(lastOrderPaySucDate)){
                    // 今天第一次支付成功，标记今日首次支付用户
                    paymentSucNewUserCount = 1L;

                    if(StringUtils.isEmpty(lastOrderPaySucDate)){
                        // 上一次支付成功时间为空，标记支付成功独立用户
                        paymentSucUniqueUserCount = 1L;
                    }
                }

                if(paymentSucNewUserCount != 0L || paymentSucUniqueUserCount != 0L){
                    TradePaymentBean tradePaymentBean = new TradePaymentBean(
                            "",
                            "",
                            "",
                            paymentSucUniqueUserCount,
                            paymentSucNewUserCount,
                            ts
                    );
                    out.collect(tradePaymentBean);
                }
            }
        });

//        processedDS.print();

        // 开窗
        AllWindowedStream<TradePaymentBean, TimeWindow> windowedDS = processedDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        // 聚合
        SingleOutputStreamOperator<TradePaymentBean> reslutDS = windowedDS.reduce(new ReduceFunction<TradePaymentBean>() {
            @Override
            public TradePaymentBean reduce(TradePaymentBean value1, TradePaymentBean value2) throws Exception {
                value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                return value1;
            }
        }, new ProcessAllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>.Context context, Iterable<TradePaymentBean> elements, Collector<TradePaymentBean> out) throws Exception {
                TradePaymentBean tradePaymentBean = elements.iterator().next();
                TimeWindow window = context.window();
                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                String curDate = DateFormatUtil.tsToDate(window.getStart());
                tradePaymentBean.setStt(stt);
                tradePaymentBean.setEdt(edt);
                tradePaymentBean.setCurDate(curDate);
                out.collect(tradePaymentBean);
            }
        });

//        reslutDS.print();

        // 写入Doris
        reslutDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_FE_NODES,
                        Constant.DORIS_USER,
                        Constant.DORIS_PASSWD,
                        Constant.DORIS_DATABASE,
                        Constant.DORIS_DWS_TRADE_PAYMENT_SUC_WINDOW));
    }
}
