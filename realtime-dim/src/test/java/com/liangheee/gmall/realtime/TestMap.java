package com.liangheee.gmall.realtime;

import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author liangheee
 * * @date 2024/10/27
 */
public class TestMap {
    public static void main(String[] args) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("1",2);
        jsonObject.put("2",3);
        ArrayList<String> list = new ArrayList<>();
        list.add("1");
        list.add("3");

        for (String s : list) {
            jsonObject.remove(s);
        }

        Set<Map.Entry<String, Object>> entries = jsonObject.entrySet();
//        for(Map.Entry<String,Object> entry : entries){
//            if("1".equals(entry.getKey())){
//                jsonObject.remove(entry.getKey());
//            }
//            System.out.println(entry.getKey() + "====" + entry.getValue());
//        }

        for(Map.Entry<String,Object> entry : entries){
            System.out.println(entry.getKey() + "====" + entry.getValue());
        }
    }
}
