package com.lxy.stream.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.lxy.stream.function.Interval4BaseJoin2BaseFunc
 * @Author xinyu.luo
 * @Date 2025/5/15 22:15
 * @description: Interval4BaseJoin2BaseFunc
 */
public class Interval4BaseJoin2BaseFunc extends ProcessJoinFunction<JSONObject,JSONObject,JSONObject> {
    @Override
    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        System.err.println("jsonObject1 -> "+ jsonObject1);
        System.err.println("jsonObject2 -> "+ jsonObject2);
    }
}
