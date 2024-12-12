package com.liangheee.gmall.publisher.service;

import com.liangheee.gmall.publisher.dto.ProvinceGmvDTO;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author liangheee
 * * @date 2024-12-12
 */
public interface TradeStatsService {
    public BigDecimal getGmv(Integer date);

    List<ProvinceGmvDTO> getProvinceGmv(Integer date);
}
