package com.lxy.realtime.app.ods;

import com.realtime.common.constant.Constant;
import com.realtime.common.utils.FlinkEnvUtils;
import com.realtime.common.utils.FlinkSinkUtil;
import com.realtime.common.utils.FlinkSourceUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkMysqlToKafka {
    @SneakyThrows
    public static void main(String[] args) {
        //todo 环境
        StreamExecutionEnvironment env = FlinkEnvUtils.getFlinkRuntimeEnv();

        //todo 获取mysqlSource
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource(Constant.HBASE_NAMESPACE, "*");

        //todo 封装
        DataStreamSource<String> mysqlSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");

        //todo sink到kafka
        mysqlSource.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DB));

        //todo 执行
        env.execute();
    }
}
