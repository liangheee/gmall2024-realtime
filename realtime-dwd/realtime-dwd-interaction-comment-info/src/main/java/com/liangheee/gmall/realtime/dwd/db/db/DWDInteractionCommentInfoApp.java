package com.liangheee.gmall.realtime.dwd.db.db;

import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.utils.SQLUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author liangheee
 * * @date 2024/11/10
 */
public class DWDInteractionCommentInfoApp {
    public static void main(String[] args) {
        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(4);
        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 检查点相关配置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        env.getCheckpointConfig().setCheckpointStorage("hdfs://mycluster:8020/gmall2024-realtime/ck/" + Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
        env.setStateBackend(new HashMapStateBackend());
        System.setProperty("HADOOP_USER_NAME","liangheee");

        // 创建动态表，从topic_db主题读取ODS数据
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "  `data` map<STRING,STRING>,\n" +
                "  `proc_time` AS PROCTIME()\n" +
                ")" + SQLUtil.getKafkaSourceConnectorParams(Constant.TOPIC_DB,Constant.BROKER_SERVERS,Constant.TOPIC_DB));
        // Table topicDb = tableEnv.sqlQuery("select * from topic_db");
        // topicDb.execute().print();

        // 过滤comment_info数据
        Table commentInfoTable = tableEnv.sqlQuery("select \n" +
                "  `data`['id'] AS id,\n" +
                "  `data`['user_id'] AS user_id,\n" +
                "  `data`['sku_id'] AS sku_id,\n" +
                "  `data`['appraise'] AS appraise,\n" +
                "  `data`['comment_txt'] AS comment_txt,\n" +
                "  `ts`,\n" +
                "  `proc_time`\n" +
                "from topic_db \n" +
                "where `database` = 'gmall2024' \n" +
                "and `table` = 'comment_info' \n" +
                "and `type` = 'insert'");
        // commentInfoTable.execute().print();
        // 注册表
        tableEnv.createTemporaryView("comment_info",commentInfoTable);

        // 创建动态表，从hbase读取base_dic字典表数据
        tableEnv.executeSql("CREATE TABLE dim_base_dic (\n" +
                " dic_code STRING,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ")" + SQLUtil.getHBaseSourceConnectorParams(Constant.HBASE_NAMESPACE,"dim_base_dic",Constant.ZOOKEEPER_QUORUM));

//        tableEnv.executeSql("select dic_code,info.dic_name from dim_base_dic").print();

        // 关联comment_info动态表和base_dic动态表
        // upsert-kafka实现了幂等写入，那么在Flink开启checkpoint的情况下，保证了至少一次语义，从而实现端到端的精确一次
        Table joinedTable = tableEnv.sqlQuery("SELECT\n" +
                "  `id`,\n" +
                "  `user_id`,\n" +
                "  `sku_id`,\n" +
                "  `appraise`,\n" +
                "  info.`dic_name` AS appraise_name,\n" +
                "  `comment_txt`,\n" +
                "  `ts`" +
                "FROM comment_info AS c\n" +
                "  JOIN dim_base_dic FOR SYSTEM_TIME AS OF c.proc_time AS d\n" +
                "    ON c.`appraise` = d.`dic_code`;");
//         joinedTable.execute().print();
//         创建upsert-kafka动态表，将关联后的结果写入kafka主题中
        tableEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO + " (\n" +
                "  id STRING,\n" +
                "  user_id STRING,\n" +
                "  sku_id STRING,\n" +
                "  appraise STRING,\n" +
                "  appraise_name STRING,\n" +
                "  comment_txt STRING,\n" +
                "  ts BIGINT,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getUpsertKafkaSinkConnectorParams(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO,Constant.BROKER_SERVERS));

        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }
}
