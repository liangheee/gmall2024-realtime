package com.liangheee.gmall.realtime.common.utils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author liangheee
 * * @date 2024/10/29
 * Flink Source创建工具类
 */
@Slf4j
public class FlinkSourceUtil {
    /**
     * 获取KafkaSource
     * @param brokerServers Kafka服务主机地址
     * @param groupId 消费者组id
     * @param topics 消费主题
     * @return KafkaSource
     */
    public static KafkaSource<String> getKafkaSource(String brokerServers,String groupId,String... topics){
        if(StringUtils.isEmpty(brokerServers) || StringUtils.isEmpty(groupId) || topics.length < 1){
            log.error("获取KafkaSource时，brokerServers：{}，groupId：{}，topics：{} 不能为空",brokerServers,groupId, Arrays.toString(topics));
            throw new RuntimeException("获取KafkaSource失败");
        }

        // KafkaSource底层创建KafkaSourceReader
        // KafkaSourceReader中保存了一个 this.offsetsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());从而来保存待提交的offset，将自动提交offset改为手动提交
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokerServers)
                .setGroupId(groupId)
                .setTopics(topics)
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
        return kafkaSource;
    }

    /**
     * 获取MysqlSource
     * @param host MySQL主机地址
     * @param port MySQL端口号
     * @param username MySQL用户名
     * @param password MySQL密码
     * @param database MySQL数据库
     * @param table MySQL数据表，参数形式：”库名.表名“
     * @return MysqlSource
     */
    public static MySqlSource<String> getMysqlSource(String host,int port,String username,String password,String database,String table){
        if(StringUtils.isEmpty(host) || StringUtils.isEmpty(database) || StringUtils.isEmpty(table)){
            log.error("获取MysqlSource时，host：{}，database：{}，table：{} 不能为空",host,database,table);
            throw new RuntimeException("获取MysqlSource失败");
        }
        Properties jdbcProperties = new Properties();
        jdbcProperties.setProperty("useSSL", "false");
        jdbcProperties.setProperty("allowPublicKeyRetrieval", "true");
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname(host)
                .port(port)
                .username(username)
                .password(password)
                .databaseList(database)
                .tableList(table)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(jdbcProperties)
                .build();
        return mysqlSource;
    }
}
