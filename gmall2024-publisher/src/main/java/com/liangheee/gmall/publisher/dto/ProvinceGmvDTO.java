package com.liangheee.gmall.publisher.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author liangheee
 * * @date 2024-12-13
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProvinceGmvDTO {
    private String name;
    private BigDecimal value;
}
