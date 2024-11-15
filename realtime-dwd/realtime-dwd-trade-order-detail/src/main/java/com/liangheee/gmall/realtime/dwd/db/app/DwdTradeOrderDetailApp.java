package com.liangheee.gmall.realtime.dwd.db.app;

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
public class DwdTradeOrderDetailApp extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetailApp().start("10014",Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    protected void handle(StreamTableEnvironment tableEnv) {
        // 设置状态保存时间，考虑两个方面（系统延迟 + 业务滞后逻辑）
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30));

        readTopicDb(tableEnv,Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);

        // 下单影响业务数据库的表：order_detail、order_info、order_detail_activity、order_detail_coupon
        // 过滤order_detail
        Table orderInfoTable = tableEnv.sqlQuery(
                "select\n" +
                "  `data`['id'] as id,\n" +
                "  `data`['user_id'] as user_id,\n" +
                "  `data`['province_id'] as province_id\n" +
                "from topic_db\n" +
                "where `database` = 'gmall2024'\n" +
                "and `table` = 'order_info'\n" +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_info",orderInfoTable);

        // 过滤order_detail
        Table orderDetailTable = tableEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['sku_name'] sku_name," +
                        "data['create_time'] create_time," +
                        "data['source_id'] source_id," +
                        "data['source_type'] source_type," +
                        "data['sku_num'] sku_num," +
                        "cast(cast(data['sku_num'] as decimal(16,2)) * " +
                        "   cast(data['order_price'] as decimal(16,2)) as String) split_original_amount," + // 分摊原始总金额
                        "data['split_total_amount'] split_total_amount," +  // 分摊总金额
                        "data['split_activity_amount'] split_activity_amount," + // 分摊活动金额
                        "data['split_coupon_amount'] split_coupon_amount," + // 分摊的优惠券金额
                        "ts " +
                        "from topic_db " +
                        "where `database`='gmall2024' " +
                        "and `table`='order_detail' " +
                        "and `type`='insert' ");
        tableEnv.createTemporaryView("order_detail",orderDetailTable);

        // 过滤order_detail_activity
        Table orderDetailActivityTable = tableEnv.sqlQuery(
                "select " +
                        "data['order_detail_id'] order_detail_id, " +
                        "data['activity_id'] activity_id, " +
                        "data['activity_rule_id'] activity_rule_id " +
                        "from topic_db " +
                        "where `database`='gmall2024' " +
                        "and `table`='order_detail_activity' " +
                        "and `type`='insert' ");
        tableEnv.createTemporaryView("order_detail_activity",orderDetailActivityTable);

        // 过滤order_detail_coupon
        Table orderDetailCouponTable = tableEnv.sqlQuery(
                "select " +
                        "data['order_detail_id'] order_detail_id, " +
                        "data['coupon_id'] coupon_id " +
                        "from topic_db " +
                        "where `database`='gmall2024' " +
                        "and `table`='order_detail_coupon' " +
                        "and `type`='insert' ");
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCouponTable);

        Table result = tableEnv.sqlQuery(
                "select " +
                        "od.id," +
                        "od.order_id," +
                        "oi.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "oi.province_id," +
                        "act.activity_id," +
                        "act.activity_rule_id," +
                        "cou.coupon_id," +
                        "date_format(od.create_time, 'yyyy-MM-dd') date_id," +  // 年月日
                        "od.create_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "od.ts " +
                        "from order_detail od " +
                        "join order_info oi on od.order_id=oi.id " +
                        "left join order_detail_activity act " +
                        "on od.id=act.order_detail_id " +
                        "left join order_detail_coupon cou " +
                        "on od.id=cou.order_detail_id ");

        tableEnv.executeSql(
                "create table " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL + "(" +
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
                        "primary key(id) not enforced " +
                        ")" + SQLUtil.getUpsertKafkaSinkConnectorParams(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,Constant.BROKER_SERVERS));

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
}
