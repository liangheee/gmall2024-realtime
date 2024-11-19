package com.liangheee.gmall.realtime.common.utils;

import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.bean.TableProcessDim;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author liangheee
 * * @date 2024/11/19
 */
public class ColumnUtil {
    public static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columns = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        entrySet.removeIf(entry -> !columns.contains(entry.getKey()));
    }
}
