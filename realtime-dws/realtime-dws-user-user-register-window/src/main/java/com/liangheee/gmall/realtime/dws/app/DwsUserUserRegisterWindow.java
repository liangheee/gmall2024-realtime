package com.liangheee.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.base.BaseApp;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.liangheee.gmall.realtime.common.utils.DateFormatUtil;
import com.liangheee.gmall.realtime.common.utils.FlinkSinkUtil;
import com.liangheee.gmall.realtime.dws.bean.UserRegisterBean;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 用户域用户注册各窗口汇总表
 * 从 DWD层用户注册表中读取数据，统计各窗口注册用户数，写入 Doris
 * @author liangheee
 * * @date 2024/12/4
 */
public class DwsUserUserRegisterWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsUserUserRegisterWindow().start(
                "10025",
                4,
                Constant.DORIS_DWS_USER_USER_REGISTER_WINDOW,
                Constant.BROKER_SERVERS,
                Constant.TOPIC_DWD_USER_REGISTER
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 将用户注册事实数据转化为专用的对象
        SingleOutputStreamOperator<UserRegisterBean> userRegisterBeanDS = kafkaStrDS.map(jsonStr -> {
            JSONObject jsonObj = JSON.parseObject(jsonStr);
            return new UserRegisterBean(
                    "",
                    "",
                    "",
                    1L,
                    DateFormatUtil.dateTimeToTs(jsonObj.getString("create_time"))
            );
        });

//        userRegisterBeanDS.print();

        // 分配水位线和时间戳
        SingleOutputStreamOperator<UserRegisterBean> userRegisterBeanWithWatermarksDS = userRegisterBeanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserRegisterBean>forMonotonousTimestamps()
                        .withTimestampAssigner((userRegisterBean, recordTimestamp) -> userRegisterBean.getTs())
        );

        // 开窗
        AllWindowedStream<UserRegisterBean, TimeWindow> windowedDS = userRegisterBeanWithWatermarksDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        // 聚合
        SingleOutputStreamOperator<UserRegisterBean> resultDS = windowedDS.reduce(new ReduceFunction<UserRegisterBean>() {
            @Override
            public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                return value1;
            }
        }, new ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>.Context context, Iterable<UserRegisterBean> elements, Collector<UserRegisterBean> out) throws Exception {
                UserRegisterBean userRegisterBean = elements.iterator().next();
                TimeWindow window = context.window();
                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                String curDate = DateFormatUtil.tsToDate(window.getStart());
                userRegisterBean.setStt(stt);
                userRegisterBean.setEdt(edt);
                userRegisterBean.setCurDate(curDate);
                out.collect(userRegisterBean);
            }
        });

//        resultDS.print();

        // 写入Doris
        resultDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_FE_NODES,
                        Constant.DORIS_USER,
                        Constant.DORIS_PASSWD,
                        Constant.DORIS_DATABASE,
                        Constant.DORIS_DWS_USER_USER_REGISTER_WINDOW));
    }
}
