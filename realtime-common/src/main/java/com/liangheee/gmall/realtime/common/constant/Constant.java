package com.liangheee.gmall.realtime.common.constant;

/**
 * @author liangheee
 * * @date 2024/10/25
 */
public class Constant {
    public static final String HADOOP_USER_NAME = "liangheee";
    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";
    public static final String TOPIC_DWD_TRAFFIC_EXCEPTION = "dwd_traffic_exception";
    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel";
    public static final String TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL = "dwd_trade_order_pay_suc_detail";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";
    public static final String TOPIC_DWD_TRADE_REFUND_PAY_SUC_DETAIL = "dwd_trade_refund_pay_suc_detail";
    public static final String TOPIC_DWD_TOOL_COUPON_GET = "dwd_tool_coupon_get";
    public static final String TOPIC_DWD_TOOL_COUPON_USE = "dwd_tool_coupon_use";
    public static final String TOPIC_DWD_INTERACTION_FAVOR_ADD = "dwd_interaction_favor_add";
    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";
    public static final String DORIS_DWS_TRAFFIC_SEARCH_KEYWORD_PAGE_VIEW_WINDOW = "dws_traffic_search_keyword_page_view_window";
    public static final String DORIS_DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW = "dws_traffic_vc_ch_ar_is_new_page_view_window";
    public static final String DORIS_DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW = "dws_traffic_home_detail_page_view_window";
    public static final String DORIS_DWS_USER_USER_REGISTER_WINDOW = "dws_user_user_register_window";
    public static final String DORIS_DWS_USER_USER_LOGIN_WINDOW = "dws_user_user_login_window";
    public static final String DORIS_DWS_TRADE_CART_ADD_UU_WINDOW = "dws_trade_cart_add_uu_window";
    public static final String DORIS_DWS_TRADE_PAYMENT_SUC_WINDOW = "dws_trade_payment_suc_window";
    public static final String DORIS_DWS_TRADE_ORDER_WINDOW = "dws_trade_order_window";
    public static final String DORIS_DWS_TRADE_SKU_ORDER_WINDOW = "dws_trade_sku_order_window";
    public static final String BROKER_SERVERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    public static final String ZOOKEEPER_QUORUM = "hadoop102,hadoop103,hadoop104:2181";
    public static final String MYSQL_HOST = "hadoop102";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER = "root";
    public static final String MYSQL_PASSWD = "123456";
    public static final String MYSQL_URL = "jdbc:mysql://hadoop102:3306?useSSL=false";
    public static final String HBASE_HOST = "hadoop102,hadoop103,hadoop104";
    public static final String HBASE_NAMESPACE = "gmall2024";
    public static final String DORIS_FE_NODES = "hadoop102:7030";
    public static final String DORIS_PASSWD = "aaaaaa";
    public static final String DORIS_USER = "root";
    public static final String DORIS_DATABASE = "gmall2024_realtime";
    public static final String REDIS_HOST = "hadoop102";
    public static final int REDIS_PORT = 6379;
}
