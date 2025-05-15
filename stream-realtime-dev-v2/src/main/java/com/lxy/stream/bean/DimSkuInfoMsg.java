package com.lxy.stream.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Package com.lxy.stream.bean.DimSkuInfoMsg
 * @Author xinyu.luo
 * @Date 2025/5/15 22:08
 * @description: DimSkuInfoMsg
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimSkuInfoMsg implements Serializable {
    private String skuid;
    private String spuid;
    private String c3id;
    private String tname;
}
