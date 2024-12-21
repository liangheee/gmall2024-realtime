package com.liangheee.gmall.publisher.util;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;

/**
 * @author liangheee
 * * @date 2024-12-12
 */
public class DateFormatUtil {
    public static Integer now(){
        String now = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(now);
    }
}
