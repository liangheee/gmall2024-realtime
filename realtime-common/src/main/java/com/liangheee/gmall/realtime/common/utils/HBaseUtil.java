package com.liangheee.gmall.realtime.common.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import com.liangheee.gmall.realtime.common.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * 操作HBase的工具类
 * @author liangheee
 * * @date 2024/10/28
 */
@Slf4j
public class HBaseUtil {

    /**
     * 获取HBase连接
     * @return HBase连接
     * @throws IOException
     */
    public static Connection getHBaseConnection() {
        try {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", Constant.HBASE_HOST);
            Connection conn = ConnectionFactory.createConnection(conf);
            log.info("获取HBase连接成功");
            return conn;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭HBase连接
     * @param conn HBase连接
     * @throws IOException
     */
    public static void closeHBaseConnection(Connection conn) {
        if(conn != null && !conn.isClosed()){
            try {
                conn.close();
                log.info("关闭HBase连接成功");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    public static AsyncConnection getAsyncHBaseConnection(){
        try {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", Constant.HBASE_HOST);
            AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(conf).get();
            log.info("获取HBase异步连接成功");
            return asyncConnection;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void closeAsyncHBaseConnection(AsyncConnection asyncConnection){
        if(asyncConnection != null && !asyncConnection.isClosed()){
            try {
                asyncConnection.close();
                log.info("关闭HBase异步连接成功");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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
        if(StringUtils.isEmpty(namespace) || StringUtils.isEmpty(table) || StringUtils.isAllEmpty(columnFamilies)){
            log.error("创建HBase表时，namespace：{},table：{}、columFamilies：{} 不能为空",namespace,table,Arrays.toString(columnFamilies));
            return;
        }

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
        if(StringUtils.isEmpty(namespace) || StringUtils.isEmpty(table)){
            log.error("创建HBase表时，namespace：{}，table：{} 不能为空",namespace,table);
            return;
        }

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

    /**
     * 删除HBase中的一行数据
     * @param conn HBase连接
     * @param namespace HBase命名空间
     * @param table HBase表
     * @param rowKey HBase rowkey
     */
    public static void deleteRow(Connection conn,String namespace,String table,String rowKey){
        if(StringUtils.isEmpty(namespace) || StringUtils.isEmpty(table) || StringUtils.isEmpty(rowKey)){
            log.error("创建HBase表时，namespace：{}，table：{}，rowKey：{} 不能为空",namespace,table,rowKey);
            return;
        }

        TableName tableName = TableName.valueOf(Bytes.toBytes(namespace), Bytes.toBytes(table));
        try (Table connTable = conn.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            connTable.delete(delete);
            log.info("HBase的namespace：{}下的表：{}，删除rowKey：{}的数据行成功",namespace,table,rowKey);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 向HBase中添加或修改数据行
     * @param conn HBase连接
     * @param namespace HBase命名空间
     * @param table HBase表
     * @param columnFamily HBase列族
     * @param rowKey HBase rowkey
     * @param jsonObj 写入数据json对象
     */
    public static void putRow(Connection conn, String namespace, String table, String columnFamily, String rowKey, JSONObject jsonObj){
        if(StringUtils.isEmpty(namespace) || StringUtils.isEmpty(table) || StringUtils.isEmpty(columnFamily) || StringUtils.isEmpty(rowKey) || jsonObj == null){
            log.error("创建HBase表时，namespace：{}，table：{}，columnFamily：{}，rowKey：{} 写入数据jsonObj不能为空",namespace,table,columnFamily,rowKey);
            return;
        }
        TableName tableName = TableName.valueOf(Bytes.toBytes(namespace), Bytes.toBytes(table));
        try (Table connTable = conn.getTable(tableName)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            Set<Map.Entry<String, Object>> entries = jsonObj.entrySet();
            for (Map.Entry<String, Object> entry : entries) {
                String columnName = entry.getKey();
                Object columnValue = entry.getValue();
                if(columnValue != null){
                    put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(columnValue.toString()));
                    connTable.put(put);
                    log.info("HBase的namespace：{}下的表：{}，添加rowKey：{}的数据行成功",namespace,table,rowKey);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据RowKey获取数据行
     * @param conn HBase连接
     * @param namespace 名称空间
     * @param table 表明
     * @param rowKey 主键
     * @param clz 输出结果类型
     * @param convertToCamel 是否转换为小驼峰命名
     * @return 返回封装结果
     * @param <T> 返回封装结果类型
     */
    public static <T> T getRow(Connection conn, String namespace, String table, String rowKey, Class<T> clz, boolean convertToCamel){
        TableName tableName = TableName.valueOf(namespace, table);
        try(Table connTable = conn.getTable(tableName)){
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = connTable.get(get);

            if(result.isEmpty()){
                return null;
            }

            T obj = clz.newInstance();
            for (Cell cell : result.listCells()) {
                String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                if(convertToCamel){
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                }
                BeanUtils.setProperty(obj,columnName,columnValue);
            }
            return obj;
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }


    public static <T> T getRowAsync(AsyncConnection conn, String namespace, String table, String rowKey, Class<T> clz, boolean convertToCamel){
        TableName tableName = TableName.valueOf(namespace, table);
        AsyncTable<AdvancedScanResultConsumer> asyncTable = conn.getTable(tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        try {
            Result result = asyncTable.get(get).get();

            if(result.isEmpty()){
                return null;
            }

            T obj = clz.newInstance();
            for (Cell cell : result.listCells()) {
                String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                if(convertToCamel){
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                }
                BeanUtils.setProperty(obj,columnName,columnValue);
            }
            return obj;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException {
        Connection conn = getHBaseConnection();
        JSONObject jsonObj = getRow(conn, Constant.HBASE_NAMESPACE, "dim_sku_info", "9", JSONObject.class, false);
        System.out.println(jsonObj);
        closeHBaseConnection(conn);
    }

}
