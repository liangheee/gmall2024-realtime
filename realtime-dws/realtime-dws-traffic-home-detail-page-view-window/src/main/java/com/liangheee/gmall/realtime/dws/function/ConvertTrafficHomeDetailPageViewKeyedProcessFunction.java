package com.liangheee.gmall.realtime.dws.function;

import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.utils.DateFormatUtil;
import com.liangheee.gmall.realtime.dws.bean.TrafficHomeDetailPageViewBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author liangheee
 * * @date 2024/12/3
 */
public class ConvertTrafficHomeDetailPageViewKeyedProcessFunction extends KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean> {
    private ValueState<String> homePageFirstVisitDate;
    private ValueState<String> goodsDetailFirstVisitDate;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<String> homePageFirstVisitDateStateDescriptor = new ValueStateDescriptor<>("home-page-first-visit-state", Types.STRING);
        homePageFirstVisitDateStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1))
                .neverReturnExpired()
                .updateTtlOnCreateAndWrite()
                .build());
        homePageFirstVisitDate = getRuntimeContext().getState(homePageFirstVisitDateStateDescriptor);

        ValueStateDescriptor<String> goodsDetailFirstVisitDateStateDescriptor = new ValueStateDescriptor<>("goods-detail-first-visit-state", Types.STRING);
        goodsDetailFirstVisitDateStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1))
                .neverReturnExpired()
                .updateTtlOnCreateAndWrite()
                .build());
        goodsDetailFirstVisitDate = getRuntimeContext().getState(goodsDetailFirstVisitDateStateDescriptor);

    }

    @Override
    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context context, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
        String pageId = pageJsonObj.getString("page_id");

        Long ts = jsonObj.getLong("ts");
        String curDate = DateFormatUtil.tsToDate(ts);

        Long homeUvCt = 0L, goodsDetailUvCt = 0L;
        if ("home".equals(pageId)) {
            if (StringUtils.isEmpty(homePageFirstVisitDate.value()) || !homePageFirstVisitDate.value().equals(curDate)) {
                homeUvCt = 1L;
                homePageFirstVisitDate.update(curDate);
            }
        } else {
            if (StringUtils.isEmpty(goodsDetailFirstVisitDate.value()) || !goodsDetailFirstVisitDate.value().equals(curDate)) {
                goodsDetailUvCt = 1L;
                goodsDetailFirstVisitDate.update(curDate);
            }
        }

        if (homeUvCt != 0L || goodsDetailUvCt != 0L) {
            TrafficHomeDetailPageViewBean trafficHomeDetailPageViewBean = new TrafficHomeDetailPageViewBean(
                    "",
                    "",
                    "",
                    homeUvCt,
                    goodsDetailUvCt,
                    ts
            );
            out.collect(trafficHomeDetailPageViewBean);
        }
    }
}
