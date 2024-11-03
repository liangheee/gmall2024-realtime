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
    public static final String BROKER_SERVERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    public static final String MYSQL_HOST = "hadoop102";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER = "root";
    public static final String MYSQL_PASSWD = "123456";
    public static final String MYSQL_URL = "jdbc:mysql://hadoop102:3306?useSSL=false";
    public static final String HBASE_HOST = "hadoop102,hadoop103,hadoop104";
    public static final String HBASE_NAMESPACE = "gmall2024";
}
