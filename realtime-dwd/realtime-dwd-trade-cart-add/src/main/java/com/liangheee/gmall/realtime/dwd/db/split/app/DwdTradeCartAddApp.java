package com.liangheee.gmall.realtime.dwd.db.split.app;

import com.liangheee.gmall.realtime.common.base.BaseSQLApp;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.utils.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author liangheee
 * * @date 2024/11/15
 */
public class DwdTradeCartAddApp extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeCartAddApp().start("10013",4,Constant.TOPIC_DWD_TRADE_CART_ADD);
    }

    @Override
    protected void handle(StreamTableEnvironment tableEnv) {
        // 读取ODS数据
        readTopicDb(tableEnv,Constant.TOPIC_DWD_TRADE_CART_ADD);

        // 过滤加购数据
        Table cartAddTable = tableEnv.sqlQuery("select\n" +
                "  `data`['id'] as id,\n" +
                "  `data`['user_id'] as user_id,\n" +
                "  `data`['sku_id'] as sku_id,\n" +
                "  if(`old` is null,cast(`data`['sku_num'] as INTEGER), cast(`data`['sku_num'] as INTEGER) - cast(`old`['sku_num'] as INTEGER)) as sku_num,\n" +
                "  `ts`\n" +
                "from topic_db\n" +
                "where `database` = 'gmall2024' \n" +
                "and `table` = 'cart_info' \n" +
                "and `type` = 'insert' \n" +
                "or (`type` = 'update' and cast(`data`['sku_num'] as INTEGER) > cast(`old`['sku_num'] as INTEGER));");

        // 写入Kafka，创建upsert-kafka动态表
        tableEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_TRADE_CART_ADD + " (\n" +
                "  id STRING,\n" +
                "  user_id STRING,\n" +
                "  sku_id STRING,\n" +
                "  sku_num BIGINT,\n" +
                "  ts BIGINT,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getUpsertKafkaSinkConnectorParams(Constant.TOPIC_DWD_TRADE_CART_ADD,Constant.BROKER_SERVERS));

        cartAddTable.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
}
