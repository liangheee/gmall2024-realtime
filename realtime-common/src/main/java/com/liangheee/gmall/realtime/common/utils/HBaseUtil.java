package com.liangheee.gmall.realtime.common.utils;

import com.liangheee.gmall.realtime.common.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author liangheee
 * * @date 2024/10/28
 * 操作HBase的工具类
 */
@Slf4j
public class HBaseUtil {

    /**
     * 获取HBase连接
     * @return HBase连接
     * @throws IOException
     */
    public static Connection getHBaseConnection() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", Constant.HBASE_HOST);
        Connection conn = ConnectionFactory.createConnection(conf);
        log.info("获取HBase连接成功");
        return conn;
    }

    /**
     * 关闭HBase连接
     * @param conn HBase连接
     * @throws IOException
     */
    public static void closeHBaseConnection(Connection conn) throws IOException {
        if(conn != null && !conn.isClosed()){
            conn.close();
            log.info("关闭HBase连接成功");
        }
    }

    /**
     * 创建HBase表
     * @param conn HBase连接
     * @param namespace HBase命名空间
     * @param table HBase表
     * @param columnFamilies HBase列族
     */
    public static void createHBaseTable(Connection conn,String namespace,String table,String... columnFamilies){
        try (Admin admin = conn.getAdmin()) {
            TableName tableName = TableName.valueOf(Bytes.toBytes(namespace),Bytes.toBytes(table));
            if(admin.tableExists(tableName)){
                log.warn("HBase的namespace：{}中已经存在表：{}",namespace,table);
                return;
            }
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
            for (String columnFamily : columnFamilies) {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily)).build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }
            admin.createTable(tableDescriptorBuilder.build());
            log.info("HBase的namespace：{}创建表：{}成功",namespace,table);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 删除HBase表
     * @param conn HBase连接
     * @param namespace HBase命名空间
     * @param table HBase表
     */
    public static void deleteHBaseTable(Connection conn,String namespace,String table){
        try (Admin admin = conn.getAdmin()) {
            TableName tableName = TableName.valueOf(Bytes.toBytes(namespace),Bytes.toBytes(table));
            if(!admin.tableExists(tableName)){
                log.warn("HBase的namespace：{}要删除的表：{}不存在",namespace,table);
                return;
            }
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            log.info("HBase的namespace：{}删除表：{}成功",namespace,table);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
