package com.liangheee.gmall.realtime.dws.function;

import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.utils.DateFormatUtil;
import com.liangheee.gmall.realtime.dws.bean.TrafficPageViewBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;

/**
 * @author liangheee
 * * @date 2024/12/3
 */
public class ConvertTrafficPageViewBeanRichMapFunction extends RichMapFunction<JSONObject, TrafficPageViewBean> {

    private ValueState<String> accessDateValueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("access-date-value-state", Types.STRING);
        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1))
                .neverReturnExpired()
                .updateTtlOnCreateAndWrite()
                .build());
        accessDateValueState = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public TrafficPageViewBean map(JSONObject jsonObj) throws Exception {
        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
        Long ts = jsonObj.getLong("ts");

        String curDate = DateFormatUtil.tsToDate(ts);
        Long uvCt = 0L;
        if (StringUtils.isEmpty(accessDateValueState.value()) || accessDateValueState.value().equals(curDate)) {
            uvCt = 1L;
            accessDateValueState.update(curDate);
        }

        String lastPageId = pageJsonObj.getString("last_page_id");
        Long svCt = 0L;
        if (StringUtils.isEmpty(lastPageId)) {
            svCt = 1L;
        }

        Long durSum = pageJsonObj.getLong("during_time");

        return new TrafficPageViewBean(
                "",
                "",
                "",
                commonJsonObj.getString("vc"),
                commonJsonObj.getString("ch"),
                commonJsonObj.getString("ar"),
                commonJsonObj.getString("is_new"),
                uvCt,
                svCt,
                1L,
                durSum,
                ts
        );
    }
}
