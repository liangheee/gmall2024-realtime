package com.liangheee.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.base.BaseApp;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.liangheee.gmall.realtime.common.utils.DateFormatUtil;
import com.liangheee.gmall.realtime.common.utils.FlinkSinkUtil;
import com.liangheee.gmall.realtime.dws.bean.TrafficPageViewBean;
import com.liangheee.gmall.realtime.dws.function.ConvertTrafficPageViewBeanRichMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表
 * @author liangheee
 * * @date 2024/12/3
 */
public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTrafficVcChArIsNewPageViewWindow().start(
                "10022",
                4,
                Constant.DORIS_DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW,
                Constant.BROKER_SERVERS,
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 将页面日志数据jsonStr转化为jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        
        // 按照mid设备号分组
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjDS.keyBy(jsonObj -> {
            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
            return commonJsonObj.getString("mid");
        });
        
        // 通过状态编程获取当日独立访客数，并且转换为专用对象TrafficPageViewBean
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewBeanDS = midKeyedDS.map(new ConvertTrafficPageViewBeanRichMapFunction());

        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewBeanWithWatermarkDS = trafficPageViewBeanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TrafficPageViewBean>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                            @Override
                            public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );

        // 按照专用对象TrafficPageViewBean中的维度vc、ch、ar、is_new分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyedStream = trafficPageViewBeanWithWatermarkDS.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean value) throws Exception {
                return Tuple4.of(value.getVc(),value.getCh(),value.getAr(),value.getIsNew());
            }
        });

        // 开窗
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowedDS = keyedStream.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        // 聚合
        SingleOutputStreamOperator<TrafficPageViewBean> resultDS = windowedDS.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                return value1;
            }
        }, new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void process(Tuple4<String, String, String, String> key, ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>.Context context, Iterable<TrafficPageViewBean> elements, Collector<TrafficPageViewBean> out) throws Exception {
                TrafficPageViewBean trafficPageViewBean = elements.iterator().next();
                TimeWindow window = context.window();
                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                String curDate = DateFormatUtil.tsToDate(window.getStart());
                trafficPageViewBean.setStt(stt);
                trafficPageViewBean.setEdt(edt);
                trafficPageViewBean.setCur_date(curDate);
                out.collect(trafficPageViewBean);
            }
        });

//        resultDS.print();

        // 写入Doris
        SingleOutputStreamOperator<String> resultJsonStrDS = resultDS.map(new BeanToJsonStrMapFunction<>());
        resultJsonStrDS.sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_FE_NODES,
                Constant.DORIS_USER,
                Constant.DORIS_PASSWD,
                Constant.DORIS_DATABASE,
                Constant.DORIS_DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW));
    }
}
