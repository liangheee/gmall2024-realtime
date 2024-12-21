package com.liangheee.gmall.realtime.dws.function;

import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.utils.DateFormatUtil;
import com.liangheee.gmall.realtime.dws.bean.UserLoginBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author liangheee
 * * @date 2024/12/4
 */
public class ConvertUserLoginBeanKeyedProcessFunction extends KeyedProcessFunction<String, JSONObject, UserLoginBean> {
    private ValueState<String> lastLoginDateSate;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("last-login-date-sate", Types.STRING);
        lastLoginDateSate = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context context, Collector<UserLoginBean> out) throws Exception {
        Long ts = jsonObj.getLong("ts");
        String curDate = DateFormatUtil.tsToDate(ts);
        String lastLoginDate = this.lastLoginDateSate.value();
        lastLoginDateSate.update(curDate);

        Long uuCt = 0L, backCt = 0L;
        if (StringUtils.isEmpty(lastLoginDate)) {
            uuCt = 1L;
        } else {
            if (!lastLoginDate.equals(curDate)) {
                uuCt = 1L;
            }

            long tsDiff = ts - DateFormatUtil.dateToTs(lastLoginDate);
            if (tsDiff >= 8L * 24 * 60 * 60 * 1000) {
                backCt = 1L;
            }
        }

        if (uuCt != 0L || backCt != 0L) {
            UserLoginBean userLoginBean = new UserLoginBean(
                    "",
                    "",
                    "",
                    backCt,
                    uuCt,
                    ts
            );
            out.collect(userLoginBean);
        }
    }
}
