package com.liangheee.gmall.publisher.service.impl;

import com.liangheee.gmall.publisher.bean.ProvinceGmv;
import com.liangheee.gmall.publisher.dto.ProvinceGmvDTO;
import com.liangheee.gmall.publisher.mapper.TradeStatsMapper;
import com.liangheee.gmall.publisher.service.TradeStatsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author liangheee
 * * @date 2024-12-12
 */
@Service
@Slf4j
public class TradeStatsServiceImpl implements TradeStatsService {
    @Autowired
    private TradeStatsMapper tradeStatsMapper;

    @Override
    public BigDecimal getGmv(Integer date) {
        return tradeStatsMapper.selectGmv(date);

    }

    @Override
    public List<ProvinceGmvDTO> getProvinceGmv(Integer date) {
        List<ProvinceGmv> provinceGmvList = tradeStatsMapper.selectProvinceGmv(date);
        List<ProvinceGmvDTO> provinceGmvDTOList = provinceGmvList.stream().map(provinceGmv -> new ProvinceGmvDTO(provinceGmv.getProvince(), provinceGmv.getAmount()))
                .collect(Collectors.toList());
        return provinceGmvDTOList;
    }
}
