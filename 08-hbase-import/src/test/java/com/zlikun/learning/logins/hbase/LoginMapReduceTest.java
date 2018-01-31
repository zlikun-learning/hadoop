package com.zlikun.learning.logins.hbase;

import com.zlikun.learning.logins.TblRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-01-30 13:38
 */
public class LoginMapReduceTest {

    @Test
    public void map() throws IOException {
        // 准备数据
        final TblRecord record = new TblRecord();
        record.userId = 1L;
        record.loginTime = 1517306026L;
        record.appType = "ZHSW_QUICK";
        record.appPlatform = "ANDROID";
        record.appVersion = "1.0.0";
        record.appVersionNumber = 12;

        final byte[] rowKeyBytes = Bytes.toBytes(String.format("%012d", record.loginTime / 1000) + String.format("%012d", record.userId));
        MapDriver<LongWritable, TblRecord, ImmutableBytesWritable, KeyValue> driver = MapDriver.newMapDriver(new LoginMapper());
        final byte[] familyUser = Bytes.toBytes("user");
        final byte[] familyApp = Bytes.toBytes("app");

        // 配置KeyValue序列化
        Configuration conf = driver.getConfiguration();
        conf.setStrings("io.serializations",
                conf.get("io.serializations"),
                KeyValueSerialization.class.getName());

        // 执行测试
        driver.withInput(new LongWritable(1), record)
                .withOutput(new ImmutableBytesWritable(rowKeyBytes),
                        new KeyValue(rowKeyBytes, familyUser, Bytes.toBytes("userId"), Bytes.toBytes(record.userId)))
                .withOutput(new ImmutableBytesWritable(rowKeyBytes),
                        new KeyValue(rowKeyBytes, familyUser, Bytes.toBytes("loginTime"), Bytes.toBytes(record.loginTime / 1000)))
                .withOutput(new ImmutableBytesWritable(rowKeyBytes),
                        new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("appType"), Bytes.toBytes(record.appType)))
                .withOutput(new ImmutableBytesWritable(rowKeyBytes),
                        new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("appPlatform"), Bytes.toBytes(record.appPlatform)))
                .withOutput(new ImmutableBytesWritable(rowKeyBytes),
                        new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("appVersion"), Bytes.toBytes(record.appVersion)))
                .withOutput(new ImmutableBytesWritable(rowKeyBytes),
                        new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("appVersionNumber"), Bytes.toBytes(record.appVersionNumber)))
                .runTest();

    }

    @Test
    public void reduce() throws IOException {
        long userId = 1L;
        long loginTime = 1517306026L;

        final byte[] rowKeyBytes = Bytes.toBytes(String.format("%012d", loginTime / 1000) + String.format("%012d", userId));
        final byte[] familyUser = Bytes.toBytes("user");

        ReduceDriver<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, KeyValue> driver = ReduceDriver.newReduceDriver(new LoginReducer());
        // 配置KeyValue序列化
        Configuration conf = driver.getConfiguration();
        conf.setStrings("io.serializations",
                conf.get("io.serializations"),
                KeyValueSerialization.class.getName());

        driver.withInput(new ImmutableBytesWritable(rowKeyBytes),
                        Arrays.asList(new KeyValue(rowKeyBytes, familyUser, Bytes.toBytes("userId"), Bytes.toBytes(userId))))
                .withOutput(new ImmutableBytesWritable(rowKeyBytes),
                        new KeyValue(rowKeyBytes, familyUser, Bytes.toBytes("userId"), Bytes.toBytes(userId)))
                .runTest();
    }

}