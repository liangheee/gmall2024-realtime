package com.liangheee.gmall.realtime.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.bean.TableProcessDim;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.utils.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.*;

/**
 * @author liangheee
 * * @date 2024/10/23
 * DIM维度层的处理
 */
@Slf4j
public class DimApp {
    public static void main(String[] args) throws Exception {
        // 1.创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.设置并行度
        env.setParallelism(4);

        // 3.开启检查点，配置检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(30)));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://mycluster:8020/gmall2024-realtime/ck");
        System.setProperty("HADOOP_USER_NAME","liangheee");

        // 4.读取业务数据
        // TODO 隐藏细节：maxwell得config.properties中已经配置了业务数据得分区器分区规则，按照数据的主键计算分区
        String groupId = "topic_db_group";
        // KafkaSource底层创建KafkaSourceReader
        // KafkaSourceReader中保存了一个 this.offsetsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());从而来保存待提交的offset，将自动提交offset改为手动提交
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.BROKER_SERVERS)
                .setGroupId(groupId)
                .setTopics(Constant.TOPIC_DB)
//                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setStartingOffsets(OffsetsInitializer.latest())
                // SimpleStringSchema底层在对序列化数据byte[]时，底层直接采用的new String()，构造器第一个参数byte[]，加了注解@NotNull
                // 也就是说如果反序列化时，传入的byte数组为null，那么就会报错
                // 因此为了容错性，我们需要自定义反序列化器
//                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if(message != null){
                            return new String(message);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                })
                .build();

        DataStreamSource<String> topicDbDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "topic_db_kafka_source");

        // 5.将读取到的业务数据jsonStr 转化为 jsonObj，并且做一些简单的etl
        SingleOutputStreamOperator<JSONObject> topicDbJsonObjDS = topicDbDS.filter(
                jsonStr -> {
                    try{
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String database = jsonObj.getString("database");
                        String type = jsonObj.getString("type");
                        String data = jsonObj.getString("data");

                        return "gmall2024".equals(database) && (
                                "insert".equals(type)
                                || "update".equals(type)
                                || "delete".equals(type)
                                || "bootstrap-insert".equals(type)
                        ) && data != null && data.length() > 2;
                    }catch (Exception e){
                        log.warn("非法json数据，无法解析");
                        return false;
                    }
                }
        ).map(JSON::parseObject);

//        topicDbJsonObjDS.print();


        // 6.FlinkCDC读取DIM配置数据
        Properties jdbcProperties = new Properties();
        jdbcProperties.setProperty("useSSL", "false");
        jdbcProperties.setProperty("allowPublicKeyRetrieval", "true");
        MySqlSource<String> dimConfigMysqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList("gmall2024_config")
                .tableList("gmall2024_config.table_process_dim")
                .username(Constant.MYSQL_USERNAME)
                .password(Constant.MYSQL_PASSWD)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(jdbcProperties)
                .build();

        DataStreamSource<String> dimConfigDS = env.fromSource(dimConfigMysqlSource, WatermarkStrategy.noWatermarks(), "dim_config_source")
                .setParallelism(1);
        // op: r {"before":null,"after":{"source_table":"base_category1","sink_table":"dim_base_category1","sink_family":"info","sink_columns":"id,name","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1730102832300,"transaction":null}
        // op: c {"before":null,"after":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"a","sink_row_key":"a"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1730103013000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":"5c385dbc-4ef7-11ee-9fa1-000c29fd57a2:2298293","file":"mysql-bin.000060","pos":452,"row":0,"thread":8,"query":null},"op":"c","ts_ms":1730103013262,"transaction":null}
        // op: d {"before":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"a","sink_row_key":"a"},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1730103052000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":"5c385dbc-4ef7-11ee-9fa1-000c29fd57a2:2298294","file":"mysql-bin.000060","pos":789,"row":0,"thread":8,"query":null},"op":"d","ts_ms":1730103052641,"transaction":null}
        // op: u {"before":{"source_table":"activity_info","sink_table":"dim_activity_info","sink_family":"info","sink_columns":"id,activity_name,activity_type,activity_desc,start_time,end_time,create_time","sink_row_key":"id"},"after":{"source_table":"activity_info1","sink_table":"dim_activity_info","sink_family":"info","sink_columns":"id,activity_name,activity_type,activity_desc,start_time,end_time,create_time","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1730103082000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":"5c385dbc-4ef7-11ee-9fa1-000c29fd57a2:2298295","file":"mysql-bin.000060","pos":1135,"row":0,"thread":8,"query":null},"op":"u","ts_ms":1730103082214,"transaction":null}
//        dimConfigDS.print();

        // 7.转换读取的DIM配置流的数据格式为专用对象,并根据配置流中的op字段判断新增或者删除HBase数据
        SingleOutputStreamOperator<TableProcessDim> tableProcessDimDS = dimConfigDS.map(
                new RichMapFunction<String, TableProcessDim>() {
                    Connection conn = null;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        conn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(conn);
                    }

                    @Override
                    public TableProcessDim map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim = null;
                        if ("d".equals(op)) {
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                            String sinkTable = tableProcessDim.getSinkTable();
                            // 删除HBase配置表
                            HBaseUtil.deleteHBaseTable(conn,Constant.HBASE_NAMESPACE,sinkTable);
                        } else if ("u".equals(op)) {
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                            String sinkTable = tableProcessDim.getSinkTable();
                            String sinkFamily = tableProcessDim.getSinkFamily();
                            String[] columnFamilies = sinkFamily.split(",");
                            // 更新HBase配置表Schema
                            HBaseUtil.deleteHBaseTable(conn,Constant.HBASE_NAMESPACE,sinkTable);
                            HBaseUtil.createHBaseTable(conn,Constant.HBASE_NAMESPACE,sinkTable,columnFamilies);
                        } else {
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                            String sinkTable = tableProcessDim.getSinkTable();
                            String sinkFamily = tableProcessDim.getSinkFamily();
                            String[] columnFamilies = sinkFamily.split(",");
                            // 创建HBase配置表
                            HBaseUtil.createHBaseTable(conn,Constant.HBASE_NAMESPACE,sinkTable,columnFamilies);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);

//        tableProcessDimDS.print();

//         8.广播配置流
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<>("table-process-dim-config", Types.STRING, Types.POJO(TableProcessDim.class));
        BroadcastStream<TableProcessDim> dimConfigBS = tableProcessDimDS.broadcast(mapStateDescriptor);

        // 9.业务流关联配置流，从业务数据中过滤维度数据
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectedStream = topicDbJsonObjDS.connect(dimConfigBS);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimBizDS = connectedStream.process(new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {

            HashMap<String,TableProcessDim> map;

            @Override
            public void open(Configuration parameters) throws Exception {
                map = new HashMap<>();
                // 通过jdbc读取mysql维度配置表信息

            }

            @Override
            public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
                // 从业务数据中过滤维度数据
                String table = jsonObj.getString("table");
                ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
                TableProcessDim tableProcessDim = broadcastState.get(table);
                if(tableProcessDim != null){
                    // 当前是维度数据，进行简单ETL，往下游传递
                    JSONObject dataJsonObj = jsonObj.getJSONObject("data");
                    // 根据HBase配置表中的sink_columns字段删除data中不需要的数据
                    deleteNotNeedColumns(dataJsonObj,tableProcessDim);

                    // 业务数据添加操作类型op
                    String op = jsonObj.getString("type");
                    dataJsonObj.put("op",op);

                    collector.collect(Tuple2.of(dataJsonObj,tableProcessDim));
                }
            }

            @Override
            public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
                // 处理广播状态的变化
                String op = tableProcessDim.getOp();
                String sourceTable = tableProcessDim.getSourceTable();
                BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(mapStateDescriptor);
                if("d".equals(op)){
                    broadcastState.remove(sourceTable);
                } else {
                    broadcastState.put(sourceTable,tableProcessDim);
                }
            }
        });

        dimBizDS.print();

        env.execute();
    }

    private static void deleteNotNeedColumns(JSONObject dataJsonObj, TableProcessDim tableProcessDim) {
        String sinkColumns = tableProcessDim.getSinkColumns();
        List<String> columns = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        entrySet.removeIf(entry -> !columns.contains(entry.getKey()));
    }
}
