package com.liangheee.gmall.publisher.controller;

import com.liangheee.gmall.publisher.dto.ChannelUvDTO;
import com.liangheee.gmall.publisher.service.TrafficStatsService;
import com.liangheee.gmall.publisher.util.DateFormatUtil;
import com.liangheee.gmall.publisher.vo.ChannelUvVo;
import com.liangheee.gmall.publisher.vo.ResultVo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author liangheee
 * * @date 2024-12-13
 */
@RestController
@RequestMapping("/traffic")
public class TrafficStatsController {

    @Autowired
    private TrafficStatsService trafficStatsService;

    @GetMapping("/ch/uv")
    public ResultVo getChUvCt(@RequestParam(name = "date",defaultValue = "0") Integer date, @RequestParam(name = "limit",defaultValue = "5") Integer limit){
        if(date == 0){
            date = DateFormatUtil.now();
        }
        ChannelUvDTO channelUvDTO = trafficStatsService.getChUvCt(date, limit);
        return ResultVo.ok().data(new ChannelUvVo(channelUvDTO.getCategories(),channelUvDTO.getSeries()));
    }


}
