package com.liangheee.gmall.realtime.common.utils;

import com.google.common.base.CaseFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 满足JDBC协议的工具类
 * @author liangheee
 * * @date 2024/10/28
 */
@Slf4j
public class JdbcUtil {
    /**
     * 获取JDBC连接
     * @param driver 数据库驱动名称
     * @param url 数据库url
     * @param user 数据库用户名
     * @param password 数据库密码
     * @return 数据库连接
     * @throws Exception
     */
    public static Connection getJdbcConnection(String driver,String url,String user,String password) throws Exception {
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(url, user, password);
        log.info("创建JDBC连接成功");
        return conn;
    }

    /**
     * 关闭JDBC连接
     * @param conn 数据库连接
     * @throws SQLException
     */
    public static void closeJdbcConnection(Connection conn) throws SQLException {
        if(conn != null && !conn.isClosed()){
            conn.close();
            log.info("关闭JDBC连接成功");
        }
    }

    /**
     * 查询数据库数据
     * @param conn 数据库连接
     * @param sql 执行sql语句
     * @param clz 返回数据元素类型
     * @param convertToCamel 是否转换驼峰命名法
     * @return 数据集列表
     * @param <T> 数据元素泛型
     * @throws Exception
     */
    public static <T> List<T> queryList(Connection conn,String sql,Class<T> clz,boolean convertToCamel) throws Exception {
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();

        ArrayList<T> list = new ArrayList<>();
        while(rs.next()){
            T obj = clz.newInstance();
            for(int i = 1;i <= metaData.getColumnCount();i++){
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(i);
                if(convertToCamel){
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                }
                BeanUtils.copyProperty(obj,columnName,columnValue);
            }
            list.add(obj);
        }

        rs.close();
        ps.close();
        return list;
    }
}
