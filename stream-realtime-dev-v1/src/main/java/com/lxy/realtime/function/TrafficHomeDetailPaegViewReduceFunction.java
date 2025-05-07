package com.lxy.realtime.function;

import com.realtime.common.bean.TrafficHomeDetailPageViewBean;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @Package com.lxy.realtime.function.SkuOrderWindowReduceFunction
 * @Author xinyu.luo
 * @Date 2025/5/7 21:16
 * @description: SkuOrderWindowReduceFunction
 */
public class TrafficHomeDetailPaegViewReduceFunction implements ReduceFunction<TrafficHomeDetailPageViewBean> {
    @Override
    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) {
        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
        return value1;
    }
}
