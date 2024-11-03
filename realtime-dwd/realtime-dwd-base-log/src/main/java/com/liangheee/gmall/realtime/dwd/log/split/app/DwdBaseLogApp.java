package com.liangheee.gmall.realtime.dwd.log.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.base.BaseApp;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.utils.FlinkSinkUtil;
import com.liangheee.gmall.realtime.dwd.log.split.function.FixedAccessFlagRichMapFunction;
import com.liangheee.gmall.realtime.dwd.log.split.function.SplitProcessFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 日志分流
 * @author liangheee
 * * @date 2024/10/30
 */
@Slf4j
public class DwdBaseLogApp extends BaseApp {

    public static void main(String[] args) throws Exception {
        new DwdBaseLogApp().start("10011",4,"dwd_base_log", Constant.BROKER_SERVERS,Constant.TOPIC_LOG);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 转换数据类型为通用数据类型，并做简单的ETL，将异常格式的日志数据写入异常格式Kafka主题
        String EXCEPTION = "exception";
        OutputTag<String> exceptionTag = new OutputTag<>(EXCEPTION, Types.STRING);
        SingleOutputStreamOperator<JSONObject> logDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        try{
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            collector.collect(jsonObj);
                        }catch (Exception e){
                            log.error("日志分流解析原始日志数据为JSON格式出现异常");
                            context.output(exceptionTag,jsonStr);
                        }
                    }
                }
        );

        // 将异常日志格式数据写入Kafka主题
        KafkaSink<String> exceptionLogKafkaSink = FlinkSinkUtil.getKafkaSink(Constant.BROKER_SERVERS, Constant.TOPIC_DWD_TRAFFIC_EXCEPTION);
        SideOutputDataStream<String> exceptionLogDS = logDS.getSideOutput(exceptionTag);
//        exceptionLogDS.print("exceptionLogDS：");
//        logDS.print("logDS：");
        exceptionLogDS.sinkTo(exceptionLogKafkaSink);

        // 修复新老访客标记
        KeyedStream<JSONObject, String> logKeyedDS = logDS.keyBy(jsonOj -> jsonOj.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> fixedNewOrOldAccessFlagDS = logKeyedDS.map(new FixedAccessFlagRichMapFunction());
//        fixedNewOrOldAccessFlagDS.print();

        // 将不同类型日志分流
        OutputTag<String> startOutputTag = new OutputTag<>("start", Types.STRING);
        OutputTag<String> dispalyOutputTag = new OutputTag<>("display", Types.STRING);
        OutputTag<String> actionOutputTag = new OutputTag<>("action", Types.STRING);
        OutputTag<String> errOutputTag = new OutputTag<>("err", Types.STRING);

        SingleOutputStreamOperator<String> pageDS = fixedNewOrOldAccessFlagDS.process(new SplitProcessFunction(startOutputTag,dispalyOutputTag,actionOutputTag,errOutputTag));

        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startOutputTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionOutputTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(dispalyOutputTag);
        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errOutputTag);

//        pageDS.print("页面日志：");
//        startDS.print("启动日志：");
//        actionDS.print("行动日志：");
//        displayDS.print("曝光日志：");
//        errDS.print("错误日志：");

        // 将不同类型的日志写入Kafka
        startDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.BROKER_SERVERS,Constant.TOPIC_DWD_TRAFFIC_START));
        pageDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.BROKER_SERVERS,Constant.TOPIC_DWD_TRAFFIC_PAGE));
        actionDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.BROKER_SERVERS,Constant.TOPIC_DWD_TRAFFIC_ACTION));
        displayDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.BROKER_SERVERS,Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        errDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.BROKER_SERVERS,Constant.TOPIC_DWD_TRAFFIC_ERR));

    }
}
