package com.liangheee.gmall.realtime.dws.test;

import com.liangheee.gmall.realtime.dws.util.IkAnalyzeUtil;


/**
 * @author liangheee
 * * @date 2024/12/2
 */
public class TestIkAnalyze {
    public static void main(String[] args) {
        String word = "小米京东自营5G限时大卖";
        System.out.println(IkAnalyzeUtil.splitWord(word));
    }
}
