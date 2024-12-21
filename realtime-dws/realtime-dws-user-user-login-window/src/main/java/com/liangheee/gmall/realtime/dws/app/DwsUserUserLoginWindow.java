package com.liangheee.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.base.BaseApp;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.liangheee.gmall.realtime.common.utils.DateFormatUtil;
import com.liangheee.gmall.realtime.common.utils.FlinkSinkUtil;
import com.liangheee.gmall.realtime.dws.bean.UserLoginBean;
import com.liangheee.gmall.realtime.dws.function.ConvertUserLoginBeanKeyedProcessFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 用户域用户登录各窗口汇总表
 * 统计七日回流用户和当日独立用户数
 * @author liangheee
 * * @date 2024/12/3
 */
public class DwsUserUserLoginWindow extends BaseApp  {
    public static void main(String[] args) throws Exception {
        new DwsUserUserLoginWindow().start(
                "10024",
                4,
                Constant.DORIS_DWS_USER_USER_LOGIN_WINDOW,
                Constant.BROKER_SERVERS,
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 将页面日志jsonStr转化为通用对象JsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // 过滤出登录行为
        SingleOutputStreamOperator<JSONObject> filteredDS = jsonObjDS.filter(jsonObj ->
                StringUtils.isNotEmpty(jsonObj.getJSONObject("common").getString("uid"))
                        && (StringUtils.isEmpty(jsonObj.getJSONObject("page").getString("last_page_id"))
                        || "login".equals(jsonObj.getJSONObject("page").getString("last_page_id"))));

//        filteredDS.print();

        // 分配watermark和时间戳
        SingleOutputStreamOperator<JSONObject> filteredWithWatermarkDS = filteredDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner((jsonObj, recordTimestamp) -> jsonObj.getLong("ts"))
        );

        // 根据uid分组
        KeyedStream<JSONObject, String> uidKeyedDS = filteredWithWatermarkDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("uid"));

        // 使用Flink状态编程，获取7天回流用户数和独立用户数，并且转化为专用对象
        SingleOutputStreamOperator<UserLoginBean> processedDS = uidKeyedDS.process(new ConvertUserLoginBeanKeyedProcessFunction());
//        processedDS.print();

        // 开窗
        AllWindowedStream<UserLoginBean, TimeWindow> windowedDS = processedDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        // 聚合
        SingleOutputStreamOperator<UserLoginBean> resultDS = windowedDS.reduce(new ReduceFunction<UserLoginBean>() {
            @Override
            public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                value2.setUuCt(value1.getUuCt() + value2.getUuCt());
                return value1;
            }
        }, new ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>.Context context, Iterable<UserLoginBean> elements, Collector<UserLoginBean> out) throws Exception {
                UserLoginBean userLoginBean = elements.iterator().next();
                TimeWindow window = context.window();
                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                String curDate = DateFormatUtil.tsToDate(window.getStart());
                userLoginBean.setStt(stt);
                userLoginBean.setEdt(edt);
                userLoginBean.setCurDate(curDate);
                out.collect(userLoginBean);
            }
        });
//        resultDS.print();

        // 写入Doris
        resultDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_FE_NODES,
                        Constant.DORIS_USER,
                        Constant.DORIS_PASSWD,
                        Constant.DORIS_DATABASE,
                        Constant.DORIS_DWS_USER_USER_LOGIN_WINDOW));
    }
}
