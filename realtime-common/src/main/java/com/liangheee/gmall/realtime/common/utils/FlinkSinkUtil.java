package com.liangheee.gmall.realtime.common.utils;

import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.bean.TableProcessDwd;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author liangheee
 * * @date 2024/11/3
 */
@Slf4j
public class FlinkSinkUtil {
    public static KafkaSink<String> getKafkaSink(String brokerServers,String topic,String... transactionId) {
        if (StringUtils.isEmpty(brokerServers) || StringUtils.isEmpty(topic)) {
            log.error("获取KafkaSink时，brokerServers：{}，topic：{} 不能为空", brokerServers, topic);
            throw new RuntimeException("获取KafkaSink失败");
        }
        KafkaSinkBuilder<String> kafkaSinkBuilder = KafkaSink.<String>builder()
                .setBootstrapServers(brokerServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        // Kafka Producer设置数据的序列化方式，可以直接使用SimpleStringSchema，不会出现反序列化时的null的问题
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build());

        if(!StringUtils.isAllEmpty(transactionId)){
            // 开启事务超时时间，大于检查点间隔超时时间，小于等于15分钟.
            kafkaSinkBuilder .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15 * 60 * 1000 + "")
                    // 开启事务，默认为NONE不开启事务
                    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    // 设置事务id前缀
                    .setTransactionalIdPrefix(transactionId[0]);
        }

        return kafkaSinkBuilder.build();
    }

    public static <T> KafkaSink<T> getKafkaSink(String brokerServers,KafkaRecordSerializationSchema<T> kafkaRecordSerializationSchema,String... transactionId) {
        if (StringUtils.isEmpty(brokerServers)) {
            log.error("获取KafkaSink时，brokerServers：{}不能为空", brokerServers);
            throw new RuntimeException("获取KafkaSink失败");
        }

        KafkaSinkBuilder<T> kafkaSinkBuilder = KafkaSink.<T>builder()
                .setBootstrapServers(brokerServers)
                .setRecordSerializer(kafkaRecordSerializationSchema);

        if(!StringUtils.isAllEmpty(transactionId)){
            // 开启事务超时时间，大于检查点间隔超时时间，小于等于15分钟.
            kafkaSinkBuilder .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15 * 60 * 1000 + "")
                    // 开启事务，默认为NONE不开启事务
                    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    // 设置事务id前缀
                    .setTransactionalIdPrefix(transactionId[0]);
        }

        return kafkaSinkBuilder.build();
    }

    public static DorisSink<String> getDorisSink(String feNodes,String user,String password,String database,String table,String... labelPrefix){
        // 补充参数校验规则

        DorisSink.Builder<String> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes(feNodes)
                .setTableIdentifier(database + "." + table)
                .setUsername(user)
                .setPassword(password);


        Properties properties = new Properties();
        // 上游是 json 写入时，需要开启配置
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");
        DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix(StringUtils.isAllEmpty(labelPrefix)? "label-doris" : labelPrefix[0]) //streamload label prefix
                .disable2PC()
                .setBufferCount(3)
                .setBufferSize(1024 * 1024)
                .setCheckInterval(3000)
                .setMaxRetries(3)
                .setStreamLoadProp(properties);

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(new SimpleStringSerializer()) //serialize according to string
                .setDorisOptions(dorisBuilder.build());

        return builder.build();
    }
}
