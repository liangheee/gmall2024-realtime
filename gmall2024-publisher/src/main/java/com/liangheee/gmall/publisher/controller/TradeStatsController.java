package com.liangheee.gmall.publisher.controller;

import com.liangheee.gmall.publisher.dto.ProvinceGmvDTO;
import com.liangheee.gmall.publisher.service.TradeStatsService;
import com.liangheee.gmall.publisher.util.DateFormatUtil;
import com.liangheee.gmall.publisher.vo.ProvinceGmvVo;
import com.liangheee.gmall.publisher.vo.ResultVo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author liangheee
 * * @date 2024-12-12
 */
@RestController
@RequestMapping("/trade")
public class TradeStatsController {

    @Autowired
    private TradeStatsService tradeStatsService;

    @GetMapping("/gmv")
    public ResultVo getGmv(@RequestParam(name = "date",defaultValue = "0") Integer date){
        if(date == 0) {
            date = DateFormatUtil.now();
        }
        BigDecimal amount = tradeStatsService.getGmv(date);
        return ResultVo.ok().data(amount);
    }

    @GetMapping("/province/gmv")
    public ResultVo getProvinceGmv(@RequestParam(name = "date",defaultValue = "0") Integer date){
        if(date == 0){
            date = DateFormatUtil.now();
        }
        List<ProvinceGmvDTO> provinceGmvDTOList = tradeStatsService.getProvinceGmv(date);
        return ResultVo.ok().data(new ProvinceGmvVo(provinceGmvDTOList,"销售额"));
    }
}
