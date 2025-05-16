package com.lxy.stream;

import com.alibaba.fastjson.JSONObject;
import com.lxy.stream.bean.DimBaseCategory;
import com.lxy.stream.function.AggregateUserDataProcessFunction;
import com.lxy.stream.function.MapDeviceAndSearchMarkModelFunc;
import com.lxy.stream.function.MapPageInfoFacility;
import com.lxy.stream.function.ProcessFilterRepeatTsData;
import com.realtime.common.constant.Constant;
import com.realtime.common.utils.FlinkSinkUtil;
import com.realtime.common.utils.FlinkSourceUtil;
import com.realtime.common.utils.JdbcUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Connection;
import java.time.Duration;
import java.util.List;

/**
 * @Package com.lxy.stream.DbCdcPageInfoBaseLabel
 * @Author xinyu.luo
 * @Date 2025/5/13 15:05
 * @description: DbCdcPageInfoBaseLabel
 */
public class DbCdcPageInfoBaseLabel {
    private static final List<DimBaseCategory> dim_base_categories;
    private static final Connection connection;

    private static final double device_rate_weight_coefficient = 0.1; // 设备权重系数
    private static final double search_rate_weight_coefficient = 0.15; // 搜索权重系数

    static {
        try {
            connection = JdbcUtils.getMySQLConnection(
                    Constant.MYSQL_URL,
                    Constant.MYSQL_USER_NAME,
                    Constant.MYSQL_PASSWORD);
            String sql = "select b3.id,                          \n" +
                    "            b3.name as b3name,              \n" +
                    "            b2.name as b2name,              \n" +
                    "            b1.name as b1name               \n" +
                    "     from realtime_v1.base_category3 as b3  \n" +
                    "     join realtime_v1.base_category2 as b2  \n" +
                    "     on b3.category2_id = b2.id             \n" +
                    "     join realtime_v1.base_category1 as b1  \n" +
                    "     on b2.category1_id = b1.id";
            dim_base_categories = JdbcUtils.queryList2(connection, sql, DimBaseCategory.class, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
    @SneakyThrows
    public static void main(String[] args) {
        System.getProperty("HADOOP_USER_NAME","root");
        //todo 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置并行度
        env.setParallelism(4);
        //todo 设置 Checkpoint 模式为精确一次 (默认)
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

        //todo 对重复数据进行去重
        //mapPageInfoDs -> > {"uid":"332","deviceInfo":{"ar":"31","uid":"332","os":"iOS","ch":"Appstore","md":"iPhone 14","vc":"v2.1.134","ba":"iPhone"},"ts":1744212138255}
        //mapPageInfoDs -> > {"uid":"332","deviceInfo":{"ar":"31","uid":"332","os":"iOS","ch":"Appstore","md":"iPhone 14","vc":"v2.1.134","ba":"iPhone"},"ts":1744212151506}
        //todo 通过状态进行去重
        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedStreamLogPageMsg.process(new ProcessFilterRepeatTsData());
        //processStagePageLogDs.print("processStagePageLogDs ->");

        // 2 min 分钟窗口
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(data -> data.getString("uid"))
                .process(new AggregateUserDataProcessFunction())
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2)
                .uid("win 2 minutes page count msg")
                .name("win 2 minutes page count msg");
        win2MinutesPageLogsDs.print("win2MinutesPageLogsDs ->");

        SingleOutputStreamOperator<JSONObject> deviceAndSearchMarkModelPageLogsDs = win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories, device_rate_weight_coefficient, search_rate_weight_coefficient));

        SingleOutputStreamOperator<String> deviceAndSearchMarkModelPageLogsDsSinkToKafka = deviceAndSearchMarkModelPageLogsDs.map(JSONObject::toString);
        //deviceAndSearchMarkModelPageLogsDsSinkToKafka.print("deviceAndSearchMarkModelPageLogsDsSinkToKafka ->");

        //deviceAndSearchMarkModelPageLogsDsSinkToKafka.sinkTo(FlinkSinkUtil.getKafkaSink("dwd_page_info_base_lebel"));

        env.disableOperatorChaining();
        env.execute("DbCdcPageInfoBaseLabel");
    }
}
