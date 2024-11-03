package com.liangheee.gmall.realtime.common.utils;

import com.liangheee.gmall.realtime.common.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Arrays;

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
                    .setTransactionalIdPrefix(topic);
        }

        return kafkaSinkBuilder.build();
    }
}
