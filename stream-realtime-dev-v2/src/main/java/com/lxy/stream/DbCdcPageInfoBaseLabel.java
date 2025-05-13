package com.lxy.stream;

import com.alibaba.fastjson.JSONObject;
import com.lxy.stream.function.AggregateUserDataProcessFunction;
import com.lxy.stream.function.MapPageInfoFacility;
import com.lxy.stream.function.ProcessFilterRepeatTsData;
import com.realtime.common.utils.FlinkSourceUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @Package com.lxy.stream.DbCdcPageInfoBaseLabel
 * @Author xinyu.luo
 * @Date 2025/5/13 15:05
 * @description: DbCdcPageInfoBaseLabel
 */
public class DbCdcPageInfoBaseLabel {
    @SneakyThrows
    public static void main(String[] args) {
        System.getProperty("HADOOP_USER_NAME","root");
        //todo 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置并行度
        env.setParallelism(1);
        //todo 设置检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //todo 获取kafka主题数据
        //todo 从 Kafka 读取 CDC 变更数据，创建一个字符串类型的数据流
        SingleOutputStreamOperator<String> kafkaSourceDs = env.fromSource(
                FlinkSourceUtil.getKafkaSource("dwd_traffic_page", "kafka_source_page_info"),
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
                "kafka_source_page_info"
        ).uid("kafka source page info").name("kafka source page info");
        //kafkaSourceDs.print("kafkaSourceDs ->");

        SingleOutputStreamOperator<JSONObject> mapKafkaSourceDs = kafkaSourceDs.map(JSONObject::parseObject);
        //mapKafkaSourceDs.print("mapKafkaSourceDs ->");

        SingleOutputStreamOperator<JSONObject> mapPageInfoDs = mapKafkaSourceDs.map(new MapPageInfoFacility())
                .uid("map page info").name("kafka source page info");
        //mapPageInfoDs.print("mapPageInfoDs -> ");

        SingleOutputStreamOperator<JSONObject> filterNotNullUidLogPageMsg = mapPageInfoDs.filter(data -> !data.getString("uid").isEmpty());
        //filterNotNullUidLogPageMsg.print("filterNotNullUidLogPageMsg ->");

        KeyedStream<JSONObject, String> keyedStreamLogPageMsg = filterNotNullUidLogPageMsg.keyBy(data -> data.getString("uid"));
        //keyedStreamLogPageMsg.print("keyedStreamLogPageMsg ->");

        //mapPageInfoDs -> > {"uid":"332","deviceInfo":{"ar":"31","uid":"332","os":"iOS","ch":"Appstore","md":"iPhone 14","vc":"v2.1.134","ba":"iPhone"},"ts":1744212138255}
        //mapPageInfoDs -> > {"uid":"332","deviceInfo":{"ar":"31","uid":"332","os":"iOS","ch":"Appstore","md":"iPhone 14","vc":"v2.1.134","ba":"iPhone"},"ts":1744212151506}
        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedStreamLogPageMsg.process(new ProcessFilterRepeatTsData());
        processStagePageLogDs.print("processStagePageLogDs ->");

        // 2 min 分钟窗口
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(data -> data.getString("uid"))
                .process(new AggregateUserDataProcessFunction())
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2)
                .uid("win 2 minutes page count msg")
                .name("win 2 minutes page count msg");
        win2MinutesPageLogsDs.print("win2MinutesPageLogsDs ->");

        env.disableOperatorChaining();
        env.execute("DbCdcPageInfoBaseLabel");
    }
}
