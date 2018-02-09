package com.zlikun.learning.dau;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-09 09:35
 */
public class DauMapReduceTest {

    @Test
    public void map() throws IOException {
        // 准备数据
        final byte[] rowKeyBytes = Bytes.toBytes("001514334531000170753157");
        final byte[] familyApp = Bytes.toBytes("app");
        Result result = Result.create(Arrays.asList(
                new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("appType"), Bytes.toBytes("ZD_STORE")),
                new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("appPlatform"), Bytes.toBytes("IOS"))
        ));

        MapDriver<ImmutableBytesWritable, Result, DateRecord, LongWritable> driver = MapDriver.newMapDriver(new DauMapper());
        // 配置KeyValue序列化
        Configuration conf = driver.getConfiguration();
        conf.setStrings("io.serializations",
                conf.get("io.serializations"),
                MutationSerialization.class.getName(),
                ResultSerialization.class.getName(),
                KeyValueSerialization.class.getName());

        // 执行测试
        driver.withInput(new ImmutableBytesWritable(rowKeyBytes), result)
                .withOutput(new DateRecord("2017-12-27", "ZD"), new LongWritable(170753157))
                .withOutput(new DateRecord("2017-12-27", "ALL"), new LongWritable(170753157))
                .runTest();

    }

    @Test
    public void reduce() throws IOException {

        ReduceDriver<DateRecord, LongWritable, DateRecord, NullWritable> driver = ReduceDriver.newReduceDriver(new DauReducer());
        // 配置KeyValue序列化
        Configuration conf = driver.getConfiguration();
        conf.setStrings("io.serializations",
                conf.get("io.serializations"),
                MutationSerialization.class.getName(),
                ResultSerialization.class.getName(),
                KeyValueSerialization.class.getName());

        driver.withInput(new DateRecord("2017-12-27", "ALL"),
                Arrays.asList(new LongWritable(170753157L)))
                .withOutput(new DateRecord("2017-12-27", "ALL"), NullWritable.get())
                .runTest();

    }

}