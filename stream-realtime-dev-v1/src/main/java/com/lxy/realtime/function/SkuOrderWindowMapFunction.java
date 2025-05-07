package com.lxy.realtime.function;

import com.alibaba.fastjson.JSONObject;
import com.realtime.common.bean.TradeSkuOrderBean;
import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;

/**
 * @Package com.lxy.realtime.function.SkuOrderWindowMapFunction
 * @Author xinyu.luo
 * @Date 2025/5/7 21:14
 * @description:
 */
public class SkuOrderWindowMapFunction implements MapFunction<JSONObject, TradeSkuOrderBean> {
    @Override
    public TradeSkuOrderBean map(JSONObject jsonObj) {
        //{"create_time":"2024-06-11 10:54:40","sku_num":"1","activity_rule_id":"5","split_original_amount":"11999.0000",
        // "split_coupon_amount":"0.0","sku_id":"19","date_id":"2024-06-11","user_id":"2998","province_id":"32",
        // "activity_id":"4","sku_name":"TCL","id":"15183","order_id":"10788","split_activity_amount":"1199.9",
        // "split_total_amount":"10799.1","ts":1718160880}

        String skuId = jsonObj.getString("sku_id");
        BigDecimal splitOriginalAmount = jsonObj.getBigDecimal("split_original_amount");
        BigDecimal splitCouponAmount = jsonObj.getBigDecimal("split_coupon_amount");
        BigDecimal splitActivityAmount = jsonObj.getBigDecimal("split_activity_amount");
        BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
        Long ts = jsonObj.getLong("ts_ms") * 1000;
        return TradeSkuOrderBean.builder()
                .skuId(skuId)
                .originalAmount(splitOriginalAmount)
                .couponReduceAmount(splitCouponAmount)
                .activityReduceAmount(splitActivityAmount)
                .orderAmount(splitTotalAmount)
                .ts_ms(ts)
                .build();
    }
}
