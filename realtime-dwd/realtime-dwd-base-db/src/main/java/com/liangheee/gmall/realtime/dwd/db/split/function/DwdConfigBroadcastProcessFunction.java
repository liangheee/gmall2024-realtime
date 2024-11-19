package com.liangheee.gmall.realtime.dwd.db.split.function;

import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.bean.TableProcessDwd;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.utils.JdbcUtil;
import com.liangheee.gmall.realtime.dwd.db.split.util.KeyUtil;
import com.mysql.cj.jdbc.Driver;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liangheee
 * * @date 2024/11/19
 */
public class DwdConfigBroadcastProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>> {

    private MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor;
    private Map<String, TableProcessDwd> dwdConfigCache = new HashMap();

    public DwdConfigBroadcastProcessFunction(MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor){
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Connection conn = JdbcUtil.getJdbcConnection(Driver.class.getName(), Constant.MYSQL_URL, Constant.MYSQL_USER, Constant.MYSQL_PASSWD);
        String sql = "select * from gmall2024_config.table_process_dwd";
        List<TableProcessDwd> result = JdbcUtil.queryList(conn, sql, TableProcessDwd.class, true);
        for (TableProcessDwd tableProcessDwd : result) {
            String key = KeyUtil.generateKey(tableProcessDwd.getSourceTable(), tableProcessDwd.getSourceType());
            dwdConfigCache.put(key, tableProcessDwd);
        }
        JdbcUtil.closeJdbcConnection(conn);
    }

    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String table = jsonObj.getString("table");
        String type = jsonObj.getString("type");
        String key = KeyUtil.generateKey(table, type);
        TableProcessDwd tableProcessDwd;
        if ((tableProcessDwd = broadcastState.get(key)) != null || (tableProcessDwd = dwdConfigCache.get(key)) != null) {
            collector.collect(Tuple2.of(jsonObj, tableProcessDwd));
        }
    }

    @Override
    public void processBroadcastElement(TableProcessDwd tableProcessDwd, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        String op = tableProcessDwd.getOp();
        String key = KeyUtil.generateKey(tableProcessDwd.getSourceTable(), tableProcessDwd.getSourceType());
        BroadcastState<String, TableProcessDwd> broadcastState = context.getBroadcastState(mapStateDescriptor);
        if ("d".equals(op)) {
            dwdConfigCache.remove(key);
            broadcastState.remove(key);
        } else {
            dwdConfigCache.put(key, tableProcessDwd);
            broadcastState.put(key, tableProcessDwd);
        }
    }
}
