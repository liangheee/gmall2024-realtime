package com.liangheee.gmall.publisher.vo;

import com.liangheee.gmall.publisher.dto.SeriesDTO;
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
public class ChannelUvVo {
    private List<String> categories;
    private List<SeriesDTO> series;
}
