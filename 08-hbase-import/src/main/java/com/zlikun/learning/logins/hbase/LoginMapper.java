package com.zlikun.learning.logins.hbase;

import com.zlikun.learning.logins.TblRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-01-30 11:35
 */
public class LoginMapper extends Mapper<LongWritable, TblRecord, ImmutableBytesWritable, KeyValue> {

    /**
     * 遍历MySQL表数据，key为表主键，值为自定义对象
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, TblRecord value, Context context) throws IOException, InterruptedException {

        // 无效数据过滤
        if (value.userId == null || value.loginTime == null || value.userId <= 0 || value.loginTime <= 0) {
            return;
        }

        // 生成RowKey：12位时间戳(精确到秒，前面补0) + 12位用户ID(左边补零)
        byte [] rowKeyBytes = Bytes.toBytes(String.format("%012d", value.loginTime / 1000) + String.format("%012d", value.userId));
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rowKeyBytes);

        // 输出 family / qualifier ，将写入到HBase
        // 登录用户相关信息 family = user，App相关信息 family = app
        byte [] familyUser = Bytes.toBytes("user");
        byte [] familyApp = Bytes.toBytes("app");

        // 用户相关信息列族
        context.write(rowKey, new KeyValue(rowKeyBytes, familyUser, Bytes.toBytes("userId"), Bytes.toBytes(value.userId)));
        context.write(rowKey, new KeyValue(rowKeyBytes, familyUser, Bytes.toBytes("loginTime"), Bytes.toBytes(value.loginTime / 1000)));
        if (StringUtils.isNotBlank(value.accountType)) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyUser, Bytes.toBytes("accountType"), Bytes.toBytes(value.accountType)));
        }
        if (StringUtils.isNotBlank(value.oauthType)) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyUser, Bytes.toBytes("oauthType"), Bytes.toBytes(value.oauthType)));
        }
        if (StringUtils.isNotBlank(value.addr)) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyUser, Bytes.toBytes("addr"), Bytes.toBytes(value.addr)));
        }

        // App相关信息列族
        if (StringUtils.isNotBlank(value.appType)) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("appType"), Bytes.toBytes(value.appType)));
        }
        if (StringUtils.isNotBlank(value.appPlatform)) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("appPlatform"), Bytes.toBytes(value.appPlatform)));
        }
        if (StringUtils.isNotBlank(value.appVersion)) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("appVersion"), Bytes.toBytes(value.appVersion)));
        }
        if (value.appVersionNumber != null && value.appVersionNumber > 0) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("appVersionNumber"), Bytes.toBytes(value.appVersionNumber)));
        }
        if (StringUtils.isNotBlank(value.deviceNumber)) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("deviceNumber"), Bytes.toBytes(value.deviceNumber)));
        }
        if (StringUtils.isNotBlank(value.imei)) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("imei"), Bytes.toBytes(value.imei)));
        }
        if (StringUtils.isNotBlank(value.operatorInfo)) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("operatorInfo"), Bytes.toBytes(value.operatorInfo)));
        }
        if (StringUtils.isNotBlank(value.kernelInfo)) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("kernelInfo"), Bytes.toBytes(value.kernelInfo)));
        }

    }

}
