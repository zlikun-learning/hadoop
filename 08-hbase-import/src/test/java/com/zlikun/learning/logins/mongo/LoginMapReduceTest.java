package com.zlikun.learning.logins.mongo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.junit.Test;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-01-30 13:38
 */
public class LoginMapReduceTest {

    @Test
    public void map() throws IOException {
        long userId = 1L;
        long loginTime = 1517306026L;

        BasicBSONObject record = new BasicBSONObject();
        record.put("userId", userId);
        record.put("loginTime", loginTime);
        record.put("appType", "ZHSW_QUICK");

        final byte[] rowKeyBytes = Bytes.toBytes(String.format("%012d", loginTime / 1000) + String.format("%012d", userId));
        MapDriver<Object, BSONObject, ImmutableBytesWritable, KeyValue> driver = MapDriver.newMapDriver(new LoginMapper());
        final byte[] familyUser = Bytes.toBytes("user");
        final byte[] familyApp = Bytes.toBytes("app");

        // 配置KeyValue序列化
        Configuration conf = driver.getConfiguration();
//        conf.setStrings("io.serializations",
//                conf.get("io.serializations"),
//                MongoSerDe.class.getName());

        // 执行测试
        driver.withInput(new LongWritable(1), record)
//                .withOutput(new ImmutableBytesWritable(rowKeyBytes),
//                        new KeyValue(rowKeyBytes, familyUser, Bytes.toBytes("userId"), Bytes.toBytes(record.getLong("userId"))))
//                .withOutput(new ImmutableBytesWritable(rowKeyBytes),
//                        new KeyValue(rowKeyBytes, familyUser, Bytes.toBytes("loginTime"), Bytes.toBytes(record.getLong("loginTime") / 1000)))
//                .withOutput(new ImmutableBytesWritable(rowKeyBytes),
//                        new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("appType"), Bytes.toBytes(record.getString("appType"))))
//                .runTest()
        ;

    }

    @Test
    public void reduce() throws IOException {
//        long userId = 1L;
//        long loginTime = 1517306026L;
//
//        final byte[] rowKeyBytes = Bytes.toBytes(String.format("%012d", loginTime / 1000) + String.format("%012d", userId));
//        final byte[] familyUser = Bytes.toBytes("user");
//
//        ReduceDriver<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, KeyValue> driver = ReduceDriver.newReduceDriver(new LoginReducer());
//        // 配置KeyValue序列化
//        Configuration conf = driver.getConfiguration();
//        conf.setStrings("io.serializations",
//                conf.get("io.serializations"),
//                KeyValueSerialization.class.getName());
//
//        driver.withInput(new ImmutableBytesWritable(rowKeyBytes),
//                        Arrays.asList(new KeyValue(rowKeyBytes, familyUser, Bytes.toBytes("userId"), Bytes.toBytes(userId))))
//                .withOutput(new ImmutableBytesWritable(rowKeyBytes),
//                        new KeyValue(rowKeyBytes, familyUser, Bytes.toBytes("userId"), Bytes.toBytes(userId)))
//                .runTest();
    }

}