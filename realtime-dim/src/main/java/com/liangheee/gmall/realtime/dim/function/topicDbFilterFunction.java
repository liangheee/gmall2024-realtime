package com.liangheee.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * 过滤
 * @author liangheee
 * * @date 2024/10/30
 */
@Slf4j
public class topicDbFilterFunction implements FilterFunction<String> {
    @Override
    public boolean filter(String jsonStr) throws Exception {
        try{
            JSONObject jsonObj = JSON.parseObject(jsonStr);
            String database = jsonObj.getString("database");
            String type = jsonObj.getString("type");
            String data = jsonObj.getString("data");

            return "gmall2024".equals(database) && (
                    "insert".equals(type)
                            || "update".equals(type)
                            || "delete".equals(type)
                            || "bootstrap-insert".equals(type)
            ) && data != null && data.length() > 2;
        }catch (Exception e){
            log.warn("非法json数据，无法解析");
            return false;
        }
    }
}
