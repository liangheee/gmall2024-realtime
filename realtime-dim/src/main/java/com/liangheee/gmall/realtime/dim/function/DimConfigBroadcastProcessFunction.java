package com.liangheee.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.bean.TableProcessDim;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.utils.JdbcUtil;
import com.mysql.cj.jdbc.Driver;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;

/**
 * @author liangheee
 * * @date 2024/10/30
 */
public class DimConfigBroadcastProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {

    private HashMap<String,TableProcessDim> dimConfigCache = new HashMap<>();

    private MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;

    private Connection jdbcConnection;
    public DimConfigBroadcastProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor){
        this.mapStateDescriptor = mapStateDescriptor;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        // 通过jdbc读取mysql维度配置表信息
        jdbcConnection = JdbcUtil.getJdbcConnection(Driver.class.getName(),Constant.MYSQL_URL,Constant.MYSQL_USER,Constant.MYSQL_PASSWD);
        String sql = "select * from gmall2024_config.table_process_dim";
        List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(jdbcConnection, sql, TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            dimConfigCache.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
    }

    @Override
    public void close() throws Exception {
        JdbcUtil.closeJdbcConnection(jdbcConnection);
    }

    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        // 从业务数据中过滤维度数据
        String table = jsonObj.getString("table");
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        TableProcessDim tableProcessDim;
        if((tableProcessDim = broadcastState.get(table)) != null ||
                (tableProcessDim = dimConfigCache.get(table)) != null){
            // 当前是维度数据，进行简单ETL，往下游传递
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");
            // 根据HBase配置表中的sink_columns字段删除data中不需要的数据
            deleteNotNeedColumns(dataJsonObj,tableProcessDim);

            // 业务数据添加操作类型op
            String type = jsonObj.getString("type");
            dataJsonObj.put("type",type);

            collector.collect(Tuple2.of(dataJsonObj,tableProcessDim));
        }
    }

    @Override
    public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        // 处理广播状态的变化
        String op = tableProcessDim.getOp();
        String sourceTable = tableProcessDim.getSourceTable();
        BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(mapStateDescriptor);
        if("d".equals(op)){
            broadcastState.remove(sourceTable);
            dimConfigCache.remove(sourceTable);
        } else {
            broadcastState.put(sourceTable,tableProcessDim);
            dimConfigCache.put(sourceTable,tableProcessDim);
        }
    }

    private static void deleteNotNeedColumns(JSONObject dataJsonObj, TableProcessDim tableProcessDim) {
        String sinkColumns = tableProcessDim.getSinkColumns();
        List<String> columns = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        entrySet.removeIf(entry -> !columns.contains(entry.getKey()));
    }
}
