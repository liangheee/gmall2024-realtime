package com.liangheee.gmall.realtime.common.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author liangheee
 * * @date 2024/12/3
 */
public class BeanToJsonStrMapFunction<T> implements MapFunction<T, String> {
    @Override
    public String map(T bean) throws Exception {
        SerializeConfig serializeConfig = new SerializeConfig();
        serializeConfig.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSON.toJSONString(bean,serializeConfig);
    }
}
