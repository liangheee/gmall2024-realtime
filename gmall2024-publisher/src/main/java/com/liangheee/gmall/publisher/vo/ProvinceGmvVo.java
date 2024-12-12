package com.liangheee.gmall.publisher.vo;

import com.liangheee.gmall.publisher.bean.ProvinceGmv;
import com.liangheee.gmall.publisher.dto.ProvinceGmvDTO;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author liangheee
 * * @date 2024-12-13
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProvinceGmvVo {
    private List<ProvinceGmvDTO> mapData;
    private String valueName;
}
