package com.lxy.stream.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Package com.lxy.stream.bean.DimBaseCategory
 * @Author xinyu.luo
 * @Date 2025/5/14 19:33
 * @description: DimBaseCategory
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimBaseCategory implements Serializable {
    private String id;
    private String b3name;
    private String b2name;
    private String b1name;
}
