package com.liangheee.gmall.publisher.service.impl;

import com.liangheee.gmall.publisher.bean.ChannelUv;
import com.liangheee.gmall.publisher.dto.ChannelUvDTO;
import com.liangheee.gmall.publisher.dto.SeriesDTO;
import com.liangheee.gmall.publisher.mapper.TrafficStatsMapper;
import com.liangheee.gmall.publisher.service.TrafficStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liangheee
 * * @date 2024-12-13
 */
@Service
public class TrafficStatsServiceImpl implements TrafficStatsService {

    @Autowired
    private TrafficStatsMapper trafficStatsMapper;

    @Override
    public ChannelUvDTO getChUvCt(Integer date, Integer limit) {
        List<ChannelUv> channelUvs = trafficStatsMapper.selectChUvCt(date, limit);
        List<String> categories = new ArrayList<>(channelUvs.size());
        List<SeriesDTO> series = new ArrayList<>(channelUvs.size());
        SeriesDTO seriesDTO = new SeriesDTO("渠道",new ArrayList<>());
        series.add(seriesDTO);
        for (ChannelUv channelUv : channelUvs) {
            categories.add(channelUv.getCh());
            seriesDTO.getData().add(channelUv.getUvCt());
        }
        return new ChannelUvDTO(categories,series);
    }
}
