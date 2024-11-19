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
public class DwdTradeOrderPaySucDetailApp extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetailApp().start("10016",4,Constant.TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL);
    }

    @Override
    protected void handle(StreamTableEnvironment tableEnv) {
        // 1. 读取下单事务事实表
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
                        "ts bigint," +
                        "et as to_timestamp_ltz(ts, 0)," +
                        "watermark for et as et - interval '3' second " +
                        ")" + SQLUtil.getKafkaSourceConnectorParams(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,Constant.BROKER_SERVERS,Constant.TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL));

        // 2. 读取 topic_db
        readTopicDb(tableEnv, Constant.TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL);

        // 3. 读取 字典表
        readDimBaseDic(tableEnv);

        // 4. 从 topic_db 中过滤 payment_info
        Table paymentInfoTable = tableEnv.sqlQuery(
                "select " +
                "data['user_id'] user_id," +
                "data['order_id'] order_id," +
                "data['payment_type'] payment_type," +
                "data['callback_time'] callback_time," +
                "`pt`," +
                "ts, " +
                "et " +
                "from topic_db " +
                "where `database`='gmall2024' " +
                "and `table`='payment_info' " +
                "and `type`='update' " +
                "and `old`['payment_status'] is not null " +
                "and `data`['payment_status']='1602' ");
        tableEnv.createTemporaryView("payment_info", paymentInfoTable);

        // 5. 3张join: interval join 无需设置 ttl
        Table result = tableEnv.sqlQuery(
                "select " +
                        "od.id order_detail_id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "pi.payment_type payment_type_code ," +
                        "dic.dic_name payment_type_name," +
                        "pi.callback_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount split_payment_amount," +
                        "pi.ts " +
                        "from payment_info pi " +
                        "join dwd_trade_order_detail od " +
                        "on pi.order_id=od.order_id " +
                        "and od.et >= pi.et - interval '30' minute " +
                        "and od.et <= pi.et + interval '5' second " +
                        "join dim_base_dic for system_time as of pi.pt as dic " +
                        "on pi.payment_type=dic.dic_code ");

        // 6. 写出到 kafka 中
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL + "(" +
                "order_detail_id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "callback_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_payment_amount string," +
                "ts bigint, " +
                "primary key(order_detail_id) not enforced " +
                ")" + SQLUtil.getUpsertKafkaSinkConnectorParams(Constant.TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL,Constant.BROKER_SERVERS));

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL);

    }
}
