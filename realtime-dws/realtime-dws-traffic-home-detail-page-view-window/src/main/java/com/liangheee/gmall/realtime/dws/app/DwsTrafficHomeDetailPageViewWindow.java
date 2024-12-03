package com.liangheee.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.base.BaseApp;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.liangheee.gmall.realtime.common.utils.DateFormatUtil;
import com.liangheee.gmall.realtime.common.utils.FlinkSinkUtil;
import com.liangheee.gmall.realtime.dws.bean.TrafficHomeDetailPageViewBean;
import com.liangheee.gmall.realtime.dws.function.ConvertTrafficHomeDetailPageViewKeyedProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 流量域首页、详情页页面浏览各窗口汇总表
 * 统计当日的首页和商品详情页独立访客数
 * @author liangheee
 * * @date 2024/12/3
 */
public class DwsTrafficHomeDetailPageViewWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTrafficHomeDetailPageViewWindow().start(
                "10023",
                4,
                Constant.DORIS_DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW,
                Constant.BROKER_SERVERS,
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 将页面日志主题数据转化为通用对象jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // 过滤出首页和商品详情页日志
        SingleOutputStreamOperator<JSONObject> homeOrGoodsDetailDS = jsonObjDS.filter(jsonObj -> {
            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
            String pageId = pageJsonObj.getString("page_id");
            return "home".equals(pageId) || "good_detail".equals(pageId);
        });
        
        // 分配水位线和抽取时间戳
        SingleOutputStreamOperator<JSONObject> homeOrGoodsDetailWithWatermarks = homeOrGoodsDetailDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.getLong("ts"))
        );


        // 根据mid分组，利用Flink状态编程判断首页或商品详情页的独立访客
        KeyedStream<JSONObject, String> midKeyedDS = homeOrGoodsDetailWithWatermarks.keyBy(elemnt -> elemnt.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> processedDS = midKeyedDS.process(new ConvertTrafficHomeDetailPageViewKeyedProcessFunction());
//        processedDS.print();

        // 开窗
        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowedDS = processedDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        // 聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultDS = windowedDS.reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
            @Override
            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                return value1;
            }
        }, new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>.Context context, Iterable<TrafficHomeDetailPageViewBean> elements, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                TrafficHomeDetailPageViewBean trafficHomeDetailPageViewBean = elements.iterator().next();
                TimeWindow window = context.window();
                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                String cureDate = DateFormatUtil.tsToDate(window.getStart());
                trafficHomeDetailPageViewBean.setStt(stt);
                trafficHomeDetailPageViewBean.setEdt(edt);
                trafficHomeDetailPageViewBean.setCurDate(cureDate);
                out.collect(trafficHomeDetailPageViewBean);
            }
        });

//        resultDS.print();

        // 写入Doris
        resultDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_FE_NODES,
                Constant.DORIS_USER,
                Constant.DORIS_PASSWD,
                Constant.DORIS_DATABASE,
                Constant.DORIS_DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW));
    }
}
