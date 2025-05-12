package com.lxy.stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lxy.stream.function.UserInfoMessageDeduplicateProcessFunc;
import com.realtime.common.utils.FlinkSourceUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @Package com.lxy.stream.DbMysqlCdcToHbase
 * @Author xinyu.luo
 * @Date 2025/5/12 13:51
 * @description: DbMysqlCdcUserInfo
 */
public class DbMysqlCdcUserInfo {
    @SneakyThrows
    public static void main(String[] args) {
        System.getProperty("HADOOP_USER_NAME","root");
        //todo 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置并行度
        env.setParallelism(1);
        //todo 设置检查点
        //env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //todo 获取kafka主题数据
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("ods_user_profile", "kafka_source_user_profile");

        SingleOutputStreamOperator<String> kafkaSourceDs = env.fromSource(kafkaSource,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, timestamp) -> JSONObject.parseObject(event).getLong("ts_ms")),
                "kafka_cdc_db_source"
        ).uid("kafka_cdc_db_source").name("kafka_cdc_db_source");
        //kafkaSourceDs.print();
        //todo 将string转换成json
        SingleOutputStreamOperator<JSONObject> kafkaUserInfoDs = kafkaSourceDs.map(JSONObject::parseObject)
                .filter(data -> data.getJSONObject("source").getString("table").equals("user_info"))
                .uid("filter user_info data")
                .name("filter user_info data");
        //kafkaUserInfoDs.print();

        SingleOutputStreamOperator<JSONObject> userInfoSupMsgDs = kafkaSourceDs.map(JSONObject::parseObject)
                .filter(data -> data.getJSONObject("source").getString("table").equals("user_info_sup_msg"))
                .uid("filter user_info_sup_msg data")
                .name("filter user_info_sup_msg data");
        //userInfoSupMsgDs.print();
        SingleOutputStreamOperator<JSONObject> userInfoDs = userInfoSupMsgDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject){
                JSONObject result = new JSONObject();
                if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                    JSONObject after = jsonObject.getJSONObject("after");
                    result.put("uid", after.getIntValue("uid"));
                    result.put("unit_height", after.getString("unit_height"));
                    result.put("create_ts", after.getLong("create_ts"));
                    result.put("weight", after.getString("weight"));
                    result.put("unit_weight", after.getString("unit_weight"));
                    result.put("height", after.getString("height"));
                    result.put("ts_ms", jsonObject.getLong("ts_ms"));
                }
                return result;
            }
        });


        SingleOutputStreamOperator<JSONObject> cdcUserInfoDs = kafkaSourceDs
                .map(jsonStr -> {
                    JSONObject json = JSON.parseObject(jsonStr);
                    JSONObject after = json.getJSONObject("after");
                    if (after != null && after.containsKey("birthday")) {
                        Integer epochDay = after.getInteger("birthday");
                        if (epochDay != null) {
                            LocalDate date = LocalDate.ofEpochDay(epochDay);
                            after.put("birthday", date.format(DateTimeFormatter.ISO_DATE));
                        }
                    }
                    return json;
                })
                .uid("convert_json")
                .name("convert_json");

        // 此处数据会存在重复 使用状态进行去重
        SingleOutputStreamOperator<JSONObject> parseCdcUserInfoDs = cdcUserInfoDs.map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject){
                        JSONObject result = new JSONObject();
                        if (jsonObject.containsKey("after")) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            if (after != null) {
                                result.put("uid", after.getLongValue("id"));
                                result.put("uname", after.getString("name"));
                                result.put("user_level", after.getString("user_level"));
                                result.put("login_name", after.getString("login_name"));
                                result.put("phone_num", after.getString("phone_num"));
                                result.put("email", after.getString("email"));
                                result.put("gender", after.getString("gender"));
                                result.put("birthday", after.getString("birthday"));
                                result.put("ts_ms", jsonObject.getLongValue("ts_ms"));
                                String birthdayStr = after.getString("birthday");
                                if (birthdayStr != null && !birthdayStr.isEmpty()) {
                                    try {
                                        LocalDate birthday = LocalDate.parse(birthdayStr, DateTimeFormatter.ISO_DATE);
                                        LocalDate currentDate = LocalDate.now(ZoneId.of("Asia/Shanghai"));
                                        int age = calculateAge(birthday, currentDate);
                                        int decade = birthday.getYear() / 10 * 10;
                                        result.put("decade", decade);
                                        result.put("age", age);
                                        String zodiac = getZodiacSign(birthday);
                                        result.put("zodiac_sign", zodiac);
                                    }catch (Exception e){
                                        result.put("age", -1);
                                        e.printStackTrace();
                                        System.err.println("日期解析失败: " + birthdayStr);
                                    }
                                }
                            }
                        }
                        return result;
                    }
                })
                .filter(data -> !data.isEmpty())
                .uid("parse json")
                .name("parse json");

        SingleOutputStreamOperator<JSONObject> CdcUserInfoSupDs = parseCdcUserInfoDs.keyBy(data -> data.getLongValue("uid"))
                .process(new UserInfoMessageDeduplicateProcessFunc());

        userInfoDs.print("userInfoDs -> ");
//        CdcUserInfoSupDs.print("CdcUserInfoSupDs ->");





        env.execute("DbusUserInfo6BaseLabel");
    }


    private static int calculateAge(LocalDate birthDate, LocalDate currentDate) {
        return Period.between(birthDate, currentDate).getYears();
    }

    private static String getZodiacSign(LocalDate birthDate) {
        int month = birthDate.getMonthValue();
        int day = birthDate.getDayOfMonth();

        // 星座日期范围定义
        if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) return "摩羯座";
        else if (month == 1 || month == 2 && day <= 18) return "水瓶座";
        else if (month == 2 || month == 3 && day <= 20) return "双鱼座";
        else if (month == 3 || month == 4 && day <= 19) return "白羊座";
        else if (month == 4 || month == 5 && day <= 20) return "金牛座";
        else if (month == 5 || month == 6 && day <= 21) return "双子座";
        else if (month == 6 || month == 7 && day <= 22) return "巨蟹座";
        else if (month == 7 || month == 8 && day <= 22) return "狮子座";
        else if (month == 8 || month == 9 && day <= 22) return "处女座";
        else if (month == 9 || month == 10 && day <= 23) return "天秤座";
        else if (month == 10 || month == 11 && day <= 22) return "天蝎座";
        else return "射手座";
    }
}
