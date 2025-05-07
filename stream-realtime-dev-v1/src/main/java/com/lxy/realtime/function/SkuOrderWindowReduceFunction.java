package com.lxy.realtime.function;

import com.realtime.common.bean.TradeSkuOrderBean;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @Package com.lxy.realtime.function.SkuOrderWindowReduceFunction
 * @Author xinyu.luo
 * @Date 2025/5/7 21:16
 * @description: SkuOrderWindowReduceFunction
 */
public class SkuOrderWindowReduceFunction implements ReduceFunction<TradeSkuOrderBean> {
    @Override
    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) {
        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
        value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
        value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
        return value1;
    }
}
