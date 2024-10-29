package com.liangheee.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.bean.TableProcessDim;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.utils.HBaseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @author liangheee
 * * @date 2024/10/30
 */
public class DimConfigRichSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
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
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
        JSONObject jsonObj = value.f0;
        TableProcessDim tableProcessDim = value.f1;

        String type = jsonObj.getString("type");
        jsonObj.remove("type");
        String sinkTable = tableProcessDim.getSinkTable();
        String sinkRowKeyColumn = tableProcessDim.getSinkRowKey();
        String rowKey = jsonObj.getString(sinkRowKeyColumn);
        if("delete".equals(type)){
            // 如果是delete，对HBase数据行进行删除操作
            HBaseUtil.deleteRow(conn, Constant.HBASE_NAMESPACE,sinkTable,rowKey);
        }else{
            // 如果是insert、update、bootstrap-insert，对HBase进行put操作
            String sinkFamily = tableProcessDim.getSinkFamily();
            HBaseUtil.putRow(conn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamily,rowKey,jsonObj);
        }
    }
}
