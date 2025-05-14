package com.lxy.stream.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.lxy.stream.bean.DimCategoryCompare
 * @Author xinyu.luo
 * @Date 2025/5/14 22:23
 * @description: DimCategoryCompare
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimCategoryCompare {
    private Integer id;
    private String categoryName;
    private String searchCategory;
}
