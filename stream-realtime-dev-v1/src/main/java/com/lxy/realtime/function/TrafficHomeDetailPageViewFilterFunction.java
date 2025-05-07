package com.lxy.realtime.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @Package com.lxy.realtime.function.TrafficHomeDetailPageViewFilterFunction
 * @Author xinyu.luo
 * @Date 2025/5/7 21:19
 * @description: TrafficHomeDetailPageViewFilterFunction
 */
public class TrafficHomeDetailPageViewFilterFunction implements FilterFunction<JSONObject> {
    @Override
    public boolean filter(JSONObject jsonObj) {
        String pageId = jsonObj.getJSONObject("page").getString("page_id");
        return "home".equals(pageId) || "good_detail".equals(pageId);
    }
}
