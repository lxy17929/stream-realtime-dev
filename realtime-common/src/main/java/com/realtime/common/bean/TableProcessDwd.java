package com.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.realtime.common.bean.TableProcessDwd
 * @Author luoxinyu
 * @Date 2025/4/9 19:33
 * @description: 1
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDwd {
    // 来源表名
    String sourceTable;

    // 来源类型
    String sourceType;

    // 目标表名
    String sinkTable;

    // 输出字段
    String sinkColumns;

    // 配置表操作类型
    String op;
}
