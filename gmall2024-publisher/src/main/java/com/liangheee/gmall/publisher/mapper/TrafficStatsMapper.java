package com.liangheee.gmall.publisher.mapper;

import com.liangheee.gmall.publisher.bean.ChannelUv;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author liangheee
 * * @date 2024-12-13
 */
public interface TrafficStatsMapper {
    @Select("SELECT ch,SUM(uv_ct) AS uvCt FROM `dws_traffic_vc_ch_ar_is_new_page_view_window` PARTITION par#{date} GROUP BY ch order by uvCt desc limit #{limit}")
    List<ChannelUv> selectChUvCt(@Param("date") Integer date, @Param("limit") Integer limit);
}
