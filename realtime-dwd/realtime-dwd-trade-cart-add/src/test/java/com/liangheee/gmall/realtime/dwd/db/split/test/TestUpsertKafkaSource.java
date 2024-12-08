package com.liangheee.gmall.realtime.dwd.db.split.test;

import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.utils.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 测试upsert-kafka作为source
 * @author liangheee
 * * @date 2024-12-05
 */
public class TestUpsertKafkaSource {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE upsertKafkaSource (\n" +
                "  order_id string,\n" +
                "  order_channel string,\n" +
                "  order_time string,\n" +
                "  pay_amount double,\n" +
                "  real_pay double,\n" +
                "  pay_time string,\n" +
                "  user_id string,\n" +
                "  user_name string,  \n" +
                "  area_id string,\n" +
                "  PRIMARY KEY (order_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'KafkaTopic',\n" +
                "  'properties.bootstrap.servers' =  '" + Constant.BROKER_SERVERS + "',\n" +
                "  'properties.group.id' = 'test-group',\n" +
                "  'key.format' = 'string',\n" +
                "  'value.format' = 'json'\n" +
                ");");


//        tableEnv.executeSql("CREATE TABLE printSink (\n" +
//                "  order_id string,\n" +
//                "  order_channel string,\n" +
//                "  order_time string,\n" +
//                "  pay_amount double,\n" +
//                "  real_pay double,\n" +
//                "  pay_time string,\n" +
//                "  user_id string,\n" +
//                "  user_name string,  \n" +
//                "  area_id string,\n" +
//                "  PRIMARY KEY (order_id) NOT ENFORCED\n" +
//                ") WITH (\n" +
//                "  'connector' = 'print'\n" +
//                ");");


//        tableEnv.executeSql("INSERT INTO printSink SELECT * FROM upsertKafkaSource;");

        tableEnv.executeSql("select * from upsertKafkaSource;").print();
    }
}
