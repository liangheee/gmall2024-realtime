package com.liangheee.gmall.publisher.mapper;

import com.liangheee.gmall.publisher.bean.ProvinceGmv;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author liangheee
 * * @date 2024-12-12
 */
public interface TradeStatsMapper {
    @Select("select sum(order_amount) as amount from dws_trade_province_order_window partition par#{date}")
    BigDecimal selectGmv(@Param("date") Integer date);

    @Select("select province_name as province,sum(order_amount) as amount from dws_trade_province_order_window partition par#{date} group by province_name")
    List<ProvinceGmv> selectProvinceGmv(@Param("date") Integer date);
}
