package com.liangheee.gmall.realtime.dwd.db.split.app;

import com.liangheee.gmall.realtime.common.base.BaseSQLApp;
import com.liangheee.gmall.realtime.common.constant.Constant;
import com.liangheee.gmall.realtime.common.utils.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author liangheee
 * * @date 2024/11/15
 */
public class DwdTradeOrderCancelApp extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderCancelApp().start("10015",4,Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }

    @Override
    protected void handle(StreamTableEnvironment tableEnv) {
        // 取消订单数据order_info join 订单明细事实表，状态超时时间要考虑业务滞后逻辑
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(15 * 60 + 10));

        // 读取ODS数据
        readTopicDb(tableEnv,Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);

        // 读取订单明细事实表数据
        tableEnv.executeSql(
                "create table dwd_trade_order_detail(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "create_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint " +
                        ")" + SQLUtil.getKafkaSourceConnectorParams(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,Constant.BROKER_SERVERS,Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));

        // 从 topic_db 过滤出订单取消数据
        Table orderCancelTable = tableEnv.sqlQuery(
                "select " +
                " `data`['id'] id, " +
                " `data`['operate_time'] operate_time, " +
                " `ts` " +
                "from topic_db " +
                "where `database`='gmall2024' " +
                "and `table`='order_info' " +
                "and `type`='update' " +
                "and `old`['order_status']='1001' " +
                "and `data`['order_status']='1003' ");
        tableEnv.createTemporaryView("order_cancel", orderCancelTable);

        Table result = tableEnv.sqlQuery(
                "select  " +
                        "od.id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "date_format(oc.operate_time, 'yyyy-MM-dd') order_cancel_date_id," +
                        "oc.operate_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "oc.ts " +
                        "from dwd_trade_order_detail od " +
                        "join order_cancel oc " +
                        "on od.order_id=oc.id ");

        // 写出
        tableEnv.executeSql(
                "create table " + Constant.TOPIC_DWD_TRADE_ORDER_CANCEL +"(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "cancel_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint," +
                        "PRIMARY KEY (id) NOT ENFORCED" +
                        ")" + SQLUtil.getUpsertKafkaSinkConnectorParams(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL,Constant.BROKER_SERVERS));

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }
}
