package com.liangheee.gmall.realtime.dwd.db.app;

import com.liangheee.gmall.realtime.common.base.BaseSQLApp;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.utils.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author liangheee
 * * @date 2024/11/10
 */
public class DwdInteractionCommentInfoApp extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfoApp().start(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }

    @Override
    protected void handle(StreamTableEnvironment tableEnv) {
        // 创建动态表，从topic_db主题读取ODS数据
        readTopicDb(tableEnv,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
        // Table topicDb = tableEnv.sqlQuery("select * from topic_db");
        // topicDb.execute().print();

        // 过滤comment_info数据
        Table commentInfoTable = tableEnv.sqlQuery("select \n" +
                "  `data`['id'] AS id,\n" +
                "  `data`['user_id'] AS user_id,\n" +
                "  `data`['sku_id'] AS sku_id,\n" +
                "  `data`['appraise'] AS appraise,\n" +
                "  `data`['comment_txt'] AS comment_txt,\n" +
                "  `ts`,\n" +
                "  `pt`\n" +
                "from topic_db \n" +
                "where `database` = 'gmall2024' \n" +
                "and `table` = 'comment_info' \n" +
                "and `type` = 'insert'");
        // commentInfoTable.execute().print();
        // 注册表
        tableEnv.createTemporaryView("comment_info",commentInfoTable);

        // 创建动态表，从hbase读取base_dic字典表数据
        readDimBaseDic(tableEnv);

//        tableEnv.executeSql("select dic_code,info.dic_name from dim_base_dic").print();

        // 关联comment_info动态表和base_dic动态表
        // upsert-kafka实现了幂等写入，那么在Flink开启checkpoint的情况下，保证了至少一次语义，从而实现端到端的精确一次
        Table joinedTable = tableEnv.sqlQuery("SELECT\n" +
                "  `id`,\n" +
                "  `user_id`,\n" +
                "  `sku_id`,\n" +
                "  `appraise`,\n" +
                "  info.`dic_name` AS appraise_name,\n" +
                "  `comment_txt`,\n" +
                "  `ts`" +
                "FROM comment_info AS c\n" +
                "  JOIN dim_base_dic FOR SYSTEM_TIME AS OF c.pt AS d\n" +
                "    ON c.`appraise` = d.`dic_code`;");
//         joinedTable.execute().print();
//         创建upsert-kafka动态表，将关联后的结果写入kafka主题中
        tableEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO + " (\n" +
                "  id STRING,\n" +
                "  user_id STRING,\n" +
                "  sku_id STRING,\n" +
                "  appraise STRING,\n" +
                "  appraise_name STRING,\n" +
                "  comment_txt STRING,\n" +
                "  ts BIGINT,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getUpsertKafkaSinkConnectorParams(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO,Constant.BROKER_SERVERS));

        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }
}
