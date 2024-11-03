package com.liangheee.gmall.realtime.dwd.log.split.function;

import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.utils.DateFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;

/**
 * @author liangheee
 * * @date 2024/11/3
 */
public class FixedAccessFlagRichMapFunction extends RichMapFunction<JSONObject, JSONObject> {
    private ValueState<String> firstAccessValueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("first_access_date", Types.STRING);
        firstAccessValueState = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public JSONObject map(JSONObject jsonObj) throws Exception {
        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
        String isNew = commonJsonObj.getString("is_new");
        String firstAccessDate = firstAccessValueState.value();
        Long ts = jsonObj.getLong("ts");
        String accessDate = DateFormatUtil.tsToDate(ts);
        if("1".equals(isNew)){
            if(StringUtils.isEmpty(firstAccessDate)){
                // 如果当前 firstAccessValueState 为空，那么不用修复访问标记，记录状态
                firstAccessValueState.update(accessDate);
            }else{
                // 如果当前 firstAccessValueState 不为空，比较 firstAccessDate 和当前访问时间是否相等，决定是否需要修复访问标记
                if(!firstAccessDate.equals(accessDate)){
                    commonJsonObj.put("is_new","0");
                }
            }
        }else{
            if(StringUtils.isEmpty(firstAccessDate)){
                // 如果当前 firstAccessValueState 为空，记录状态为前一天日期即可
                firstAccessValueState.update(DateFormatUtil.tsToDate(ts - 24L * 60 * 60 * 1000));
            }
        }
        return jsonObj;
    }
}
