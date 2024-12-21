package com.liangheee.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.base.BaseApp;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.liangheee.gmall.realtime.common.utils.DateFormatUtil;
import com.liangheee.gmall.realtime.common.utils.FlinkSinkUtil;
import com.liangheee.gmall.realtime.dws.bean.CartAddUuBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
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
 * 交易域加购各窗口汇总表
 * 从 Kafka 读取用户加购明细数据，统计各窗口加购独立用户数，写入 Doris
 * @author liangheee
 * * @date 2024/12/4
 */
public class DwsTradeCartAddUuWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeCartAddUuWindow().start(
                "10026",
                4,
                Constant.DORIS_DWS_TRADE_CART_ADD_UU_WINDOW,
                Constant.BROKER_SERVERS,
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 将加购事务事实表主题数据转化为jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);


        // 分配水位线和时间戳
        SingleOutputStreamOperator<JSONObject> jsonObjWithWatermarksDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        // FlinkCDC获取的ts单位是秒，10位数，需要转换为Flink需要的毫秒
                        .withTimestampAssigner((jsonObj, recordTimestamp) -> jsonObj.getLong("ts") * 1000L)
        );

//        jsonObjWithWatermarksDS.print();

//        // 按照用户id进行分组
        KeyedStream<JSONObject, String> userIdKeyedDS = jsonObjWithWatermarksDS.keyBy(jsonObj -> jsonObj.getString("user_id"));

        // 利用Flink状态编程判断加购独立用户数
        SingleOutputStreamOperator<CartAddUuBean> processedDS = userIdKeyedDS.process(new KeyedProcessFunction<String, JSONObject, CartAddUuBean>() {
            private ValueState<String> lastCartAddDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("last-cart-add-state", Types.STRING);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1))
                        .neverReturnExpired()
                        .updateTtlOnCreateAndWrite()
                        .build());
                lastCartAddDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, CartAddUuBean>.Context context, Collector<CartAddUuBean> out) throws Exception {
                Long ts = jsonObj.getLong("ts") * 1000L;
                String curDate = DateFormatUtil.tsToDate(ts);
                String lastCartAddDate = lastCartAddDateState.value();
                if (StringUtils.isEmpty(lastCartAddDate) || !lastCartAddDate.equals(curDate)) {
                    lastCartAddDateState.update(curDate);
                    CartAddUuBean cartAddUuBean = new CartAddUuBean(
                            "",
                            "",
                            "",
                            1L
                    );
                    out.collect(cartAddUuBean);
                }
            }
        });

//        processedDS.print();

        // 开窗
        AllWindowedStream<CartAddUuBean, TimeWindow> windowedDS = processedDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        // 聚合
        SingleOutputStreamOperator<CartAddUuBean> resultDS = windowedDS.aggregate(new AggregateFunction<CartAddUuBean, Long, CartAddUuBean>() {
            @Override
            public Long createAccumulator() {
                return 0L;
            }

            @Override
            public Long add(CartAddUuBean value, Long acc) {
                return value.getCartAddUuCt() + acc;
            }

            @Override
            public CartAddUuBean getResult(Long acc) {
                return new CartAddUuBean(
                        "",
                        "",
                        "",
                        acc);
            }

            @Override
            public Long merge(Long acc1, Long acc2) {
                return acc1 + acc2;
            }
        }, new ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>.Context context, Iterable<CartAddUuBean> elements, Collector<CartAddUuBean> out) throws Exception {
                CartAddUuBean cartAddUuBean = elements.iterator().next();
                TimeWindow window = context.window();
                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                String curDate = DateFormatUtil.tsToDate(window.getStart());
                cartAddUuBean.setStt(stt);
                cartAddUuBean.setEdt(edt);
                cartAddUuBean.setCurDate(curDate);
                out.collect(cartAddUuBean);
            }
        });

//        resultDS.print();

        // 写入Doris
        resultDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_FE_NODES,
                        Constant.DORIS_USER,
                        Constant.DORIS_PASSWD,
                        Constant.DORIS_DATABASE,
                        Constant.DORIS_DWS_TRADE_CART_ADD_UU_WINDOW));
    }
}
