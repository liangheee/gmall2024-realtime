package com.liangheee.gmall.realtime.dwd.log.split.function;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author liangheee
 * * @date 2024/11/3
 */
public class SplitProcessFunction extends ProcessFunction<JSONObject, String> {
    OutputTag<String> startOutputTag;
    OutputTag<String> dispalyOutputTag;
    OutputTag<String> actionOutputTag;
    OutputTag<String> errOutputTag;

    public SplitProcessFunction(OutputTag<String> startOutputTag,OutputTag<String> dispalyOutputTag,OutputTag<String> actionOutputTag,OutputTag<String> errOutputTag){
        this.startOutputTag = startOutputTag;
        this.dispalyOutputTag = dispalyOutputTag;
        this.actionOutputTag = actionOutputTag;
        this.errOutputTag = errOutputTag;
    }

    @Override
    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
        JSONObject errJsonObj = jsonObj.getJSONObject("err");
        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
        Long ts = jsonObj.getLong("ts");

        JSONObject outputJsonObj = new JSONObject();
        outputJsonObj.put("common", commonJsonObj);
        outputJsonObj.put("ts", ts);
        if (errJsonObj != null) {
            outputJsonObj.put("err", errJsonObj);
            context.output(errOutputTag, outputJsonObj.toJSONString());
            outputJsonObj.remove("err");
            jsonObj.remove("err");
        }

        // 启动日志和页面日志互斥
        JSONObject startJsonObj = jsonObj.getJSONObject("start");
        if (startJsonObj != null) {
            outputJsonObj.put("start", startJsonObj);
            context.output(startOutputTag, outputJsonObj.toJSONString());
            outputJsonObj.remove("start");
            jsonObj.remove("start");
            return;
        }

        // 页面日志中可能包含曝光日志
        JSONArray displays = jsonObj.getJSONArray("displays");
        if (displays != null && !displays.isEmpty()) {
            for (Object display : displays) {
                outputJsonObj.put("display", display);
                context.output(dispalyOutputTag, outputJsonObj.toJSONString());
            }
            outputJsonObj.remove("display");
            jsonObj.remove("displays");
        }

        // 页面日志中可能包含行动日志
        // 注意：行动日志的ts时间要修改为具体的行动时间
        JSONArray actions = jsonObj.getJSONArray("actions");
        if (actions != null && !actions.isEmpty()) {
            for (Object action : actions) {
                JSONObject actionJsonObj = (JSONObject) action;
                outputJsonObj.put("action", action);
                outputJsonObj.put("ts",actionJsonObj.getLong("ts"));
                context.output(actionOutputTag, outputJsonObj.toJSONString());
            }
            outputJsonObj.remove("action");
            jsonObj.remove("actions");
        }

        collector.collect(jsonObj.toJSONString());
    }
}
