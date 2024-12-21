package com.liangheee.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.bean.TableProcessDim;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.utils.HBaseUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @author liangheee
 * * @date 2024/10/30
 */
public class DimConfigRichMapFunction extends RichMapFunction<String, TableProcessDim> {
    private Connection conn = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        conn = HBaseUtil.getHBaseConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(conn);
    }

    @Override
    public TableProcessDim map(String jsonStr) throws Exception {
        JSONObject jsonObj = JSON.parseObject(jsonStr);
        String op = jsonObj.getString("op");
        TableProcessDim tableProcessDim = null;
        if ("d".equals(op)) {
            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
            String sinkTable = tableProcessDim.getSinkTable();
            // 删除HBase配置表
            HBaseUtil.deleteHBaseTable(conn, Constant.HBASE_NAMESPACE,sinkTable);
        } else if ("u".equals(op)) {
            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
            String sinkTable = tableProcessDim.getSinkTable();
            String sinkFamily = tableProcessDim.getSinkFamily();
            String[] columnFamilies = sinkFamily.split(",");
            // 更新HBase配置表Schema
            HBaseUtil.deleteHBaseTable(conn,Constant.HBASE_NAMESPACE,sinkTable);
            HBaseUtil.createHBaseTable(conn,Constant.HBASE_NAMESPACE,sinkTable,columnFamilies);
        } else {
            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
            String sinkTable = tableProcessDim.getSinkTable();
            String sinkFamily = tableProcessDim.getSinkFamily();
            String[] columnFamilies = sinkFamily.split(",");
            // 创建HBase配置表
            HBaseUtil.createHBaseTable(conn,Constant.HBASE_NAMESPACE,sinkTable,columnFamilies);
        }
        tableProcessDim.setOp(op);
        return tableProcessDim;
    }
}
