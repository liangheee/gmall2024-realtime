package com.liangheee.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.base.BaseApp;
import com.liangheee.gmall.realtime.common.bean.TableProcessDwd;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.utils.FlinkSinkUtil;
import com.liangheee.gmall.realtime.common.utils.FlinkSourceUtil;
import com.liangheee.gmall.realtime.dwd.db.split.function.DwdConfigBroadcastProcessFunction;
import com.liangheee.gmall.realtime.dwd.db.split.serializer.DwdBaseDbKafkaRecordSerializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * 简单事实表的动态分流
 *      工具域优惠卷领取事务事实表  table=coupon_use type=insert
 *      工具域优惠卷使用事务事实表  table=coupon_use type=update coupon_status=1403
 *      互动域收藏事务事实表       table=favor_info type=insert
 *      用户域用户注册事务事实表    table=user_info  type=insert
 * @author liangheee
 * * @date 2024/11/18
 */
@Slf4j
public class DwdBaseDbApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwdBaseDbApp().start(
                "10019",
                4,
                "dwd_base_db",
                Constant.BROKER_SERVERS,
                Constant.TOPIC_DB
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 对topicdb中的数据进行简单的ETL，将jsonStr转化为通用对象JsonObj
        SingleOutputStreamOperator<JSONObject> topicDbDS = odsEtl(kafkaStrDS);

        // FlinkCDC读取dwd动态分流的简单事实表配置信息
        SingleOutputStreamOperator<TableProcessDwd> tableProcessDwdDS = readDwdConfig(env);

        // 广播配置流，过滤出动态分流简单事实表
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processDS = broadcastFilterDwd(tableProcessDwdDS, topicDbDS);

        // 写入Kafka
        writeKafka(processDS);
    }

    private static SingleOutputStreamOperator<JSONObject> odsEtl(DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> topicDbDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {

            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                String type = jsonObj.getString("type");
                if(StringUtils.isEmpty(type) || type.contains("bootstrap-")){
                    return;
                }
                collector.collect(jsonObj);
            }
        });
        return topicDbDS;
    }

    private static SingleOutputStreamOperator<TableProcessDwd> readDwdConfig(StreamExecutionEnvironment env) {
        MySqlSource<String> mysqlSource = FlinkSourceUtil.getMysqlSource(
                Constant.MYSQL_HOST,
                Constant.MYSQL_PORT,
                Constant.MYSQL_USER,
                Constant.MYSQL_PASSWD,
                "gmall2024_config",
                "gmall2024_config.table_process_dwd");
        DataStreamSource<String> dwdConfigDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysql-source").setParallelism(1);
        // 配置流的jsonStr转化为专用对象
        SingleOutputStreamOperator<TableProcessDwd> tableProcessDwdDS = dwdConfigDS.map(new MapFunction<String, TableProcessDwd>() {
            @Override
            public TableProcessDwd map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                String op = jsonObj.getString("op");
                TableProcessDwd tableProcessDwd;
                if ("d".equals(op)) {
                    tableProcessDwd = jsonObj.getObject("before", TableProcessDwd.class);
                } else {
                    tableProcessDwd = jsonObj.getObject("after", TableProcessDwd.class);
                }
                tableProcessDwd.setOp(op);
                return tableProcessDwd;
            }
        }).setParallelism(1);
        return tableProcessDwdDS;
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> broadcastFilterDwd(SingleOutputStreamOperator<TableProcessDwd> tableProcessDwdDS, SingleOutputStreamOperator<JSONObject> topicDbDS) {
        // 广播配置流
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<>("dwd-config-map-state", Types.STRING, Types.POJO(TableProcessDwd.class));
        BroadcastStream<TableProcessDwd> tableProcessDwdBS = tableProcessDwdDS.broadcast(mapStateDescriptor);
        // 主流关联配置流
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectedBCS = topicDbDS.connect(tableProcessDwdBS);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processDS = connectedBCS.process(new DwdConfigBroadcastProcessFunction(mapStateDescriptor));
        return processDS;
    }

    private static void writeKafka(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processDS) {
        KafkaSink<Tuple2<JSONObject, TableProcessDwd>> kafkaSink = FlinkSinkUtil.getKafkaSink(Constant.BROKER_SERVERS, new DwdBaseDbKafkaRecordSerializationSchema());
        processDS.sinkTo(kafkaSink);
    }
}
