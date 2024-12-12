package com.liangheee.gmall.publisher.dto;

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
public class SeriesDTO {
    private String name;
    private List<Integer> data;
}
