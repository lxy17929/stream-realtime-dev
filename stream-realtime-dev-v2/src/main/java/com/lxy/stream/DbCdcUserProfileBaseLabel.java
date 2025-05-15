package com.lxy.stream;

import com.alibaba.fastjson.JSONObject;
import com.realtime.common.utils.FlinkSourceUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Package com.lxy.stream.DbCdcUserProfileBaseLabel
 * @Author xinyu.luo
 * @Date 2025/5/15 18:04
 * @description: DbCdcUserProfileBaseLabel
 */
public class DbCdcUserProfileBaseLabel {
    @SneakyThrows
    public static void main(String[] args) {
        System.getProperty("HADOOP_USER_NAME","root");
        //todo 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置并行度
        env.setParallelism(1);
        //todo 设置 Checkpoint 模式为精确一次 (默认)
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        //todo 获取kafka主题数据
        //todo 从 Kafka 读取用户标签数据，创建一个字符串类型的数据流
        SingleOutputStreamOperator<String> userInfoLabelKafkaSourceDs = env.fromSource(
                FlinkSourceUtil.getKafkaSource("dwd_user_info_label", "kafka_source_dwd_user_info_label"),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                                    JSONObject jsonObject = JSONObject.parseObject(event);
                                    if (event != null && jsonObject.containsKey("ts_ms")){
                                        try {
                                            return JSONObject.parseObject(event).getLong("ts_ms");
                                        }catch (Exception e){
                                            e.printStackTrace();
                                            System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                            return 0L;
                                        }
                                    }
                                    return 0L;
                                }
                        ),
                "kafka_source_dwd_user_info_label"
        ).uid("kafka source dwd user info label").name("kafka source dwd user info label");
        //userInfoLabelKafkaSourceDs.print("userInfoLabelKafkaSourceDs ->");

        SingleOutputStreamOperator<String> pageInfoBaseLabelKafkaSourceDs = env.fromSource(
                FlinkSourceUtil.getKafkaSource("dwd_page_info_base_lebel", "kafka_source_dwd_page_info_base_lebel_v1"),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                                    JSONObject jsonObject = JSONObject.parseObject(event);
                                    if (event != null && jsonObject.containsKey("ts_ms")){
                                        try {
                                            return JSONObject.parseObject(event).getLong("ts_ms");
                                        }catch (Exception e){
                                            e.printStackTrace();
                                            System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                            return 0L;
                                        }
                                    }
                                    return 0L;
                                }
                        ),
                "kafka_source_dwd_page_info_base_lebel"
        ).uid("kafka source dwd page info base lebel").name("kafka source  dwd page info base lebel");
        //pageInfoBaseLabelKafkaSourceDs.print("pageInfoBaseLabelKafkaSourceDs ->");

        SingleOutputStreamOperator<String> orderInfoBaseLabelKafkaSourceDs = env.fromSource(
                FlinkSourceUtil.getKafkaSource("dwd_order_info_base_label", "kafka_source_dwd_order_info_base_label"),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                                    JSONObject jsonObject = JSONObject.parseObject(event);
                                    if (event != null && jsonObject.containsKey("ts_ms")){
                                        try {
                                            return JSONObject.parseObject(event).getLong("ts_ms");
                                        }catch (Exception e){
                                            e.printStackTrace();
                                            System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                            return 0L;
                                        }
                                    }
                                    return 0L;
                                }
                        ),
                "kafka_source_dwd_order_info_base_label"
        ).uid("kafka source dwd order info base label").name("kafka source dwd order info base label");
        //orderInfoBaseLabelKafkaSourceDs.print("orderInfoBaseLabelKafkaSourceDs ->");

        KeyedStream<JSONObject, String> keyedUserInfoLabelDs = userInfoLabelKafkaSourceDs.map(JSONObject::parseObject).keyBy(data -> data.getString("uid"));
        KeyedStream<JSONObject, String> keyedPageInfoBaseLabelDs = pageInfoBaseLabelKafkaSourceDs.map(JSONObject::parseObject).keyBy(data -> data.getString("uid"));
        KeyedStream<JSONObject, String> keyedOrderInfoBaseLabelDs = orderInfoBaseLabelKafkaSourceDs.map(JSONObject::parseObject).keyBy(data -> data.getString("user_id"));
        //keyedUserInfoLabelDs.print("keyedUserInfoLabelDs ->");
        //keyedPageInfoBaseLabelDs.print("keyedPageInfoBaseLabelDs ->");
        //keyedOrderInfoBaseLabelDs.print("keyedOrderInfoBaseLabelDs ->");

        /*SingleOutputStreamOperator<JSONObject> userInfoJoinrderInfoBaseLabelDs = mapUserInfoLabelDs.keyBy(o -> o.getString("uid"))
                .intervalJoin(mapOrderInfoBaseLabelDs.keyBy(o -> o.getString("user_id")))
                .between(Time.hours(-24), Time.hours(24))
                .process(new ProcessJoinBase2And4BaseFunc());*/

        SingleOutputStreamOperator<JSONObject> userInfoJoinrderInfoBaseLabelDs = keyedUserInfoLabelDs
                .intervalJoin(keyedOrderInfoBaseLabelDs)
                .between(Time.hours(-24), Time.hours(24))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) {
                        left.putAll(right);
                        out.collect(left);
                    }
                });
        //userInfoJoinrderInfoBaseLabelDs.print();

//        SingleOutputStreamOperator<JSONObject> winUserInfoJoinrderInfoBaseLabelDs = userInfoJoinrderInfoBaseLabelDs.assignTimestampsAndWatermarks(
//                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (jsonObject, l) -> jsonObject.getLongValue("ts_ms")));
        //winUserInfoJoinrderInfoBaseLabelDs.print();

        SingleOutputStreamOperator<JSONObject> userLabelProcessDs = keyedUserInfoLabelDs.keyBy(data -> data.getString("uid"))
                .intervalJoin(keyedPageInfoBaseLabelDs)
                .between(Time.days(-3), Time.days(3))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) {
                        left.putAll(right);
                        out.collect(left);
                    }
                });

        userLabelProcessDs.print();

        env.disableOperatorChaining();
        env.execute("DbCdcOrderInfoBaseLabel");
    }
}
