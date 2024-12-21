package com.liangheee.gmall.realtime.common.base;

import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.utils.FlinkSourceUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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

import java.util.Arrays;

/**
 * Flink API模板类
 * @author liangheee
 * * @date 2024/10/30
 */
@Slf4j
public abstract class BaseApp {
    public void start(String port,int parallelism,String ckAndGroupId,String brokerServers,String... topics) throws Exception {
        if(Integer.parseInt(port) < 0 || Integer.parseInt(port) > 65535){
            log.error("创建Flink启动环境时，Flink WebUI的端口号：{}，超出合理范围0~65535",port);
            throw new RuntimeException("创建Flink环境失败");
        }

        if(parallelism <= 0){
            log.error("Flink程序并行度必须大于0");
            throw new RuntimeException("创建Flink环境失败");
        }

        if(StringUtils.isEmpty(ckAndGroupId) || StringUtils.isEmpty(brokerServers) || StringUtils.isAllEmpty(topics)){
            log.error("创建Flink启动环境时，ckAndGroupId：{}，Kafka brokerServers ：{}，topics:{}可能为空",ckAndGroupId,brokerServers, Arrays.toString(topics));
            throw new RuntimeException("创建Flink环境失败");
        }

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
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(brokerServers, ckAndGroupId, topics);

        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        // 5.处理业务逻辑
        handle(env,kafkaStrDS);

        // 6.执行env
        env.execute();
    }

    public abstract void handle(StreamExecutionEnvironment env,DataStreamSource<String> kafkaStrDS);
}
