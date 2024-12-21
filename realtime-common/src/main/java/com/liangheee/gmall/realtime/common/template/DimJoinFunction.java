package com.liangheee.gmall.realtime.common.template;

import com.alibaba.fastjson.JSONObject;

/**
 * @author liangheee
 * * @date 2024-12-08
 */
public interface DimJoinFunction<T> {
    String getDimKey(T obj);

    String getDimTable();

    void joinDim(T obj, JSONObject dimJsonObj);
}
