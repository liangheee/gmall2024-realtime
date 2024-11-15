package com.liangheee.gmall.realtime.common.base;

import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.utils.SQLUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author liangheee
 * * @date 2024/11/15
 */
public abstract class BaseSQLApp {
    public void start(String ck){
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
        env.getCheckpointConfig().setCheckpointStorage("hdfs://mycluster:8020/gmall2024-realtime/ck/" + ck);
        env.setStateBackend(new HashMapStateBackend());
        System.setProperty("HADOOP_USER_NAME","liangheee");

        handle(tableEnv);
    }

    protected abstract void handle(StreamTableEnvironment tableEnv);

    public void readTopicDb(StreamTableEnvironment tableEnv,String groupId){
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "  `data` map<STRING,STRING>,\n" +
                "  `pt` AS PROCTIME()\n" +
                ")" + SQLUtil.getKafkaSourceConnectorParams(Constant.TOPIC_DB,Constant.BROKER_SERVERS,groupId));
    }

    public void readDimBaseDic(StreamTableEnvironment tableEnv){
        tableEnv.executeSql("CREATE TABLE dim_base_dic (\n" +
                " dic_code STRING,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ")" + SQLUtil.getHBaseSourceConnectorParams(Constant.HBASE_NAMESPACE,"dim_base_dic",Constant.ZOOKEEPER_QUORUM));
    }

}
