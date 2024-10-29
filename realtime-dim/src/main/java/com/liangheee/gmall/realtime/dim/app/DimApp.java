package com.liangheee.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.base.BaseApp;
import com.liangheee.gmall.realtime.common.bean.TableProcessDim;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.utils.FlinkSourceUtil;
import com.liangheee.gmall.realtime.dim.function.DimConfigBroadcastProcessFunction;
import com.liangheee.gmall.realtime.dim.function.DimConfigRichMapFunction;
import com.liangheee.gmall.realtime.dim.function.DimConfigRichSinkFunction;
import com.liangheee.gmall.realtime.dim.function.topicDbFilterFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liangheee
 * * @date 2024/10/23
 * DIM维度层的处理
 */
@Slf4j
public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DimApp().start("10001",4,"dim_app",Constant.BROKER_SERVERS,Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 将读取到的业务数据jsonStr 转化为 jsonObj，并且做一些简单的etl
        SingleOutputStreamOperator<JSONObject> topicDbJsonObjDS = kafkaStrDS.filter(new topicDbFilterFunction())
                .map(JSON::parseObject);
        // topicDbJsonObjDS.print();

        // FlinkCDC读取DIM配置数据
        MySqlSource<String> dimConfigMysqlSource = FlinkSourceUtil.getMysqlSource(
                Constant.MYSQL_HOST,
                Constant.MYSQL_PORT,
                Constant.MYSQL_USER,
                Constant.MYSQL_PASSWD,
                "gmall2024_config",
                "gmall2024_config.table_process_dim");


        DataStreamSource<String> dimConfigDS = env.fromSource(dimConfigMysqlSource, WatermarkStrategy.noWatermarks(), "dim_config_source")
                .setParallelism(1);
        // op: r {"before":null,"after":{"source_table":"base_category1","sink_table":"dim_base_category1","sink_family":"info","sink_columns":"id,name","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1730102832300,"transaction":null}
        // op: c {"before":null,"after":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"a","sink_row_key":"a"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1730103013000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":"5c385dbc-4ef7-11ee-9fa1-000c29fd57a2:2298293","file":"mysql-bin.000060","pos":452,"row":0,"thread":8,"query":null},"op":"c","ts_ms":1730103013262,"transaction":null}
        // op: d {"before":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"a","sink_row_key":"a"},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1730103052000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":"5c385dbc-4ef7-11ee-9fa1-000c29fd57a2:2298294","file":"mysql-bin.000060","pos":789,"row":0,"thread":8,"query":null},"op":"d","ts_ms":1730103052641,"transaction":null}
        // op: u {"before":{"source_table":"activity_info","sink_table":"dim_activity_info","sink_family":"info","sink_columns":"id,activity_name,activity_type,activity_desc,start_time,end_time,create_time","sink_row_key":"id"},"after":{"source_table":"activity_info1","sink_table":"dim_activity_info","sink_family":"info","sink_columns":"id,activity_name,activity_type,activity_desc,start_time,end_time,create_time","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1730103082000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":"5c385dbc-4ef7-11ee-9fa1-000c29fd57a2:2298295","file":"mysql-bin.000060","pos":1135,"row":0,"thread":8,"query":null},"op":"u","ts_ms":1730103082214,"transaction":null}
        // dimConfigDS.print();

        // 转换读取的DIM配置流的数据格式为专用对象,并根据配置流中的op字段判断新增或者删除HBase数据
        SingleOutputStreamOperator<TableProcessDim> tableProcessDimDS = dimConfigDS.map(new DimConfigRichMapFunction()).setParallelism(1);

//        tableProcessDimDS.print();

       // 广播配置流
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<>("table-process-dim-config", Types.STRING, Types.POJO(TableProcessDim.class));
        BroadcastStream<TableProcessDim> dimConfigBS = tableProcessDimDS.broadcast(mapStateDescriptor);

        // 9.业务流关联配置流，从业务数据中过滤维度数据
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectedStream = topicDbJsonObjDS.connect(dimConfigBS);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimBizDS = connectedStream.process(new DimConfigBroadcastProcessFunction(mapStateDescriptor));

        // ({"spu_name":"小米12sultra1111","tm_id":1,"description":"小米10","id":1,"type":"update","category3_id":61},TableProcessDim(sourceTable=spu_info, sinkTable=dim_spu_info, sinkColumns=id,spu_name,description,category3_id,tm_id, sinkFamily=info, sinkRowKey=id, op=r))
//         dimBizDS.print();

        dimBizDS.addSink(new DimConfigRichSinkFunction());
    }
}
