package com.liangheee.gmall.realtime.dws.app;

import com.liangheee.gmall.realtime.common.base.BaseSQLApp;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.utils.SQLUtil;
import com.liangheee.gmall.realtime.dws.function.SplitWordUDTF;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 流量域搜索关键词粒度页面浏览各窗口汇总表
 * @author liangheee
 * * @date 2024/12/2
 */
public class DwsTrafficSearchKeywordPageViewWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new DwsTrafficSearchKeywordPageViewWindow().start(
                "10021",
                4,
                Constant.DORIS_DWS_TRAFFIC_SEARCH_KEYWORD_PAGE_VIEW_WINDOW
        );
    }

    @Override
    protected void handle(StreamTableEnvironment tableEnv) {
        // 注册分词UDTF函数
        tableEnv.createTemporarySystemFunction("ik_analyze", SplitWordUDTF.class);

        // 读取页面浏览日志主题
        tableEnv.executeSql("create table dwd_traffic_page (\n" +
                "\t`common` map<string,string>,\n" +
                "\t`page` map<string,string>,\n" +
                "\t`ts` bigint,\n" +
                "\t`et` as TO_TIMESTAMP_LTZ(`ts`,3),\n" +
                "\tWATERMARK FOR `et` AS `et`\n" +
                ")" + SQLUtil.getKafkaSourceConnectorParams(Constant.TOPIC_DWD_TRAFFIC_PAGE,Constant.BROKER_SERVERS,Constant.DORIS_DWS_TRAFFIC_SEARCH_KEYWORD_PAGE_VIEW_WINDOW));
//        tableEnv.executeSql("select * from dwd_traffic_page;").print();

        // 过滤搜索行为
        Table searchKeyWordTable = tableEnv.sqlQuery("select\n" +
                "\t`page`['item'] as keyword,\n" +
                "\t`et`\n" +
                "from dwd_traffic_page\n" +
                "where (`page`['last_page_id'] = 'search' or `page`['last_page_id'] = 'home') \n" +
                "and `page`['page_id'] is not null \n" +
                "and `page`['item_type'] = 'keyword' \n" +
                "and `page`['item'] is not null");
//        searchKeyWordTable.execute().print();
        tableEnv.createTemporaryView("search_keyword",searchKeyWordTable);

        // 对搜索关键词进行分词处理，关联处理时间
        Table splitWordTable = tableEnv.sqlQuery("select \n" +
                "\tword,\n" +
                "\tet\n" +
                "FROM search_keyword,\n" +
                "LATERAL TABLE(ik_analyze(keyword)) t(word);");
//        splitWordTable.execute().print();
        tableEnv.createTemporaryView("split_word",splitWordTable);

        // 分组、开窗、聚合
        Table resultTable = tableEnv.sqlQuery("SELECT \n" +
                "\tdate_format(window_start,'yyyy-MM-dd HH:mm:ss') as stt, \n" +
                "\tdate_format(window_end,'yyyy-MM-dd HH:mm:ss') as edt,\n" +
                "\tdate_format(window_end,'yyyyMMdd') as cur_date,\n" +
                "\tword as keyword,\n" +
                "\tCOUNT(*) AS keyword_count\n" +
                "FROM TABLE(TUMBLE(TABLE split_word, DESCRIPTOR(et), INTERVAL '10' SECONDS))\n" +
                "GROUP BY window_start, window_end,word;");
//        resultTable.execute().print();

        // 写入Doris
        tableEnv.executeSql("create table " + Constant.DORIS_DWS_TRAFFIC_SEARCH_KEYWORD_PAGE_VIEW_WINDOW + "(" +
                "  stt string, " +  // 2023-07-11 14:14:14
                "  edt string, " +
                "  cur_date string, " +
                "  keyword string, " +
                "  keyword_count bigint " +
                ")" + SQLUtil.getDorisSinkConnectorParams(Constant.DORIS_FE_NODES,Constant.DORIS_USER,Constant.DORIS_PASSWD,Constant.DORIS_DATABASE,Constant.DORIS_DWS_TRAFFIC_SEARCH_KEYWORD_PAGE_VIEW_WINDOW));
        resultTable.executeInsert(Constant.DORIS_DWS_TRAFFIC_SEARCH_KEYWORD_PAGE_VIEW_WINDOW);
    }
}
