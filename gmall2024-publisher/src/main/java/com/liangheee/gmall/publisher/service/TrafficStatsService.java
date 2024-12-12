package com.liangheee.gmall.publisher.service;

import com.liangheee.gmall.publisher.dto.ChannelUvDTO;

/**
 * @author liangheee
 * * @date 2024-12-13
 */
public interface TrafficStatsService {

    ChannelUvDTO getChUvCt(Integer date, Integer limit);
}
