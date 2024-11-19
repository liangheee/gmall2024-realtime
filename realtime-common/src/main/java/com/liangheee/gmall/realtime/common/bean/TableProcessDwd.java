package com.liangheee.gmall.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author liangheee
 * * @date 2024/11/19
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableProcessDwd {
    private String sourceTable;
    private String sourceType;
    private String sinkTable;
    private String sinkColumns;
    private String op;
}
