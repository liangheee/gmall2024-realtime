package com.liangheee.gmall.realtime.common.utils;

import com.liangheee.gmall.realtime.common.constant.Constant;

/**
 * @author liangheee
 * * @date 2024/11/11
 */
public class SQLUtil {
    public static String getKafkaSourceConnectorParams(String topic,String brokerServers,String groupId){
        String params = "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                        "  'topic' = '" + topic +"',\n" +
                        "  'properties.bootstrap.servers' = '" + brokerServers + "',\n" +
                        "  'properties.group.id' = '" + groupId + "',\n" +
//                "  'scan.startup.mode' = 'group-offsets',\n" +
//                "  'properties.auto.offset.reset' = 'latest',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'json'\n" +
                        ");";
        return params;
    }

    public static String getHBaseSourceConnectorParams(String namespace,String table,String zookeeperQuorum){
        String params = "WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = '" + namespace + ":" + table + "',\n" +
                " 'zookeeper.quorum' = '" + zookeeperQuorum + "',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '500',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 HOUR',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 HOUR'\n" +
                ");";
        return params;
    }

    public static String getUpsertKafkaSinkConnectorParams(String topic,String brokerServers){
        String params = "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + brokerServers + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ");";
        return params;
    }

    public static String getDorisSinkConnectorParams(String feNodes,String user,String password,String database,String table){
        String params = "with(" +
                " 'connector' = 'doris'," +
                " 'fenodes' = '" + feNodes + "'," +
                "  'table.identifier' = '" + database + "." + table + "'," +
                "  'username' = '" + user + "'," +
                "  'password' = '" + password + "', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
                "  'sink.properties.read_json_by_line' = 'true' " +
                ")";
        return params;
    }


}
