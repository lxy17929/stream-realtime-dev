package com.lxy.stream;

import com.realtime.common.utils.FlinkEnvUtils;
import com.realtime.common.utils.FlinkSinkUtil;
import com.realtime.common.utils.FlinkSourceUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCToKafka {
    @SneakyThrows
    public static void main(String[] args) {
        System.getProperty("HADOOP_USER_NAME","root");
        //todo 环境
        StreamExecutionEnvironment env = FlinkEnvUtils.getFlinkRuntimeEnv();

        //todo 获取mysqlSource
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("realtime_v1", "*");

        //todo 封装
        DataStreamSource<String> mysqlSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");

        mysqlSource.print();

        //todo sink到kafka
        mysqlSource.sinkTo(FlinkSinkUtil.getKafkaSink("ods_user_profile"));

        //todo 执行
        env.execute();
    }
}
