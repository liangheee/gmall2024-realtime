package com.liangheee.gmall.realtime.common.base;

import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink API模板类
 * @author liangheee
 * * @date 2024/10/30
 */
public abstract class BaseApp {
    public void start(String port,int parallelism,String ckAndGroupId,String brokerServers,String... topic) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.port",port);

        // 1.创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 2.设置并行度
        env.setParallelism(parallelism);

        // 3.开启检查点，配置检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://mycluster:8020/gmall2024-realtime/ck/" + ckAndGroupId);
        System.setProperty("HADOOP_USER_NAME", Constant.HADOOP_USER_NAME);

        // 4.读取业务数据
        // TODO 隐藏细节：maxwell得config.properties中已经配置了业务数据得分区器分区规则，按照数据的主键计算分区
        String groupId = "topic_db_group";
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(brokerServers, ckAndGroupId, topic);

        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        // 5.处理业务逻辑
        handle(env,kafkaStrDS);

        // 6.执行env
        env.execute();
    }

    public abstract void handle(StreamExecutionEnvironment env,DataStreamSource<String> kafkaStrDS);
}
