package com.liangheee.gmall.realtime.common.base;

import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.utils.SQLUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 * @author liangheee
 * * @date 2024/11/15
 */
@Slf4j
public abstract class BaseSQLApp {
    public void start(String port,String ck){
        if(Integer.parseInt(port) < 0 || Integer.parseInt(port) > 65535){
            log.error("创建Flink启动环境时，Flink WebUI的端口号：{}，超出合理范围0~65535",port);
            throw new RuntimeException("创建Flink环境失败");
        }

        if(StringUtils.isEmpty(ck)){
            log.error("创建Flink启动环境时，ck：{}为空",ck);
            throw new RuntimeException("创建Flink环境失败");
        }

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
                "  `old` map<STRING,STRING>,\n" +
                "  `pt` AS PROCTIME(),\n" +
                "   et AS to_timestamp_ltz(ts, 0),\n" +
                "  WATERMARK FOR et AS et - INTERVAL '3' SECOND\n" +
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
