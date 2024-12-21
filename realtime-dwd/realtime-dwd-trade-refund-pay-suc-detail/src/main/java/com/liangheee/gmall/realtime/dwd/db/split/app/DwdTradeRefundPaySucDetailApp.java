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
public class DwdTradeRefundPaySucDetailApp extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeRefundPaySucDetailApp().start("10018",4,Constant.TOPIC_DWD_TRADE_REFUND_PAY_SUC_DETAIL);
    }
    @Override
    protected void handle(StreamTableEnvironment tableEnv) {
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        // 读取ODS数据
        readTopicDb(tableEnv,Constant.TOPIC_DWD_TRADE_REFUND_PAY_SUC_DETAIL);

        // 读取字典表
        readDimBaseDic(tableEnv);

        // 过滤退款成功表数据
        Table refundPayment = tableEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['payment_type'] payment_type," +
                        "data['callback_time'] callback_time," +
                        "data['total_amount'] total_amount," +
                        "pt, " +
                        "ts " +
                        "from topic_db " +
                        "where `database`='gmall2024' " +
                        "and `table`='refund_payment' " +
                        "and `type`='update' " +
                        "and `old`['refund_status'] is not null " +
                        "and `data`['refund_status']='1602'");
        tableEnv.createTemporaryView("refund_payment", refundPayment);

        // 过滤退单表中退单成功数据
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select " +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['refund_num'] refund_num " +
                        "from topic_db " +
                        "where `database`='gmall2024' " +
                        "and `table`='order_refund_info' " +
                        "and `type`='update' " +
                        "and `old`['refund_status'] is not null " +
                        "and `data`['refund_status']='0705'");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // 过滤订单表中的退款成功的数据
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['user_id'] user_id," +
                        "data['province_id'] province_id " +
                        "from topic_db " +
                        "where `database`='gmall2024' " +
                        "and `table`='order_info' " +
                        "and `type`='update' " +
                        "and `old`['order_status'] is not null " +
                        "and `data`['order_status']='1006'");
        tableEnv.createTemporaryView("order_info", orderInfo);

        Table result = tableEnv.sqlQuery(
                "select " +
                        "rp.id," +
                        "oi.user_id," +
                        "rp.order_id," +
                        "rp.sku_id," +
                        "oi.province_id," +
                        "rp.payment_type," +
                        "dic.info.dic_name payment_type_name," +
                        "date_format(rp.callback_time,'yyyy-MM-dd') date_id," +
                        "rp.callback_time," +
                        "ori.refund_num," +
                        "rp.total_amount," +
                        "rp.ts " +
                        "from refund_payment rp " +
                        "join order_refund_info ori " +
                        "on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id " +
                        "join order_info oi " +
                        "on rp.order_id=oi.id " +
                        "join dim_base_dic for system_time as of rp.pt as dic " +
                        "on rp.payment_type=dic.dic_code ");

        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_REFUND_PAY_SUC_DETAIL + "(" +
                "id string," +
                "user_id string," +
                "order_id string," +
                "sku_id string," +
                "province_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "date_id string," +
                "callback_time string," +
                "refund_num string," +
                "refund_amount string," +
                "ts bigint," +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ")" + SQLUtil.getUpsertKafkaSinkConnectorParams(Constant.TOPIC_DWD_TRADE_REFUND_PAY_SUC_DETAIL,Constant.BROKER_SERVERS));

        result.executeInsert(Constant.TOPIC_DWD_TRADE_REFUND_PAY_SUC_DETAIL);
    }
}
