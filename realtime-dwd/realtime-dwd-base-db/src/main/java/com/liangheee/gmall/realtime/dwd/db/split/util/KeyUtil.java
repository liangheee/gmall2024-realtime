package com.liangheee.gmall.realtime.dwd.db.split.util;

/**
 * @author liangheee
 * * @date 2024/11/19
 */
public class KeyUtil {
    public static String generateKey(String sourceTable, String sourceType) {
        return sourceTable + ":" + sourceType;
    }
}
