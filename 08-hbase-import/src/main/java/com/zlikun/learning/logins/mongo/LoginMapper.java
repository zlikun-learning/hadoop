package com.zlikun.learning.logins.mongo;

import com.mongodb.BasicDBObject;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import java.io.IOException;
import java.text.ParseException;

/**
 * 读取Mongo数据，按HBase数据格式输出
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-01-31 11:11
 */
public class LoginMapper extends Mapper<Object, BSONObject, ImmutableBytesWritable, KeyValue> {

    final String regex = "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$";
    final String pattern = "yyyy-MM-dd HH:mm:ss";

    @Override
    protected void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
        BasicDBObject obj = (BasicDBObject) value;
        long userId = obj.getLong("user_id");
        String loginTime = obj.getString("loginTime");  // 2018-01-30 00:00:05
        if (userId <= 0L || loginTime == null || !loginTime.matches(regex)) {
            return;
        }
        // 时间格式转换
        long loginTimeSeconds = 0;
        try {
            loginTimeSeconds = DateUtils.parseDate(loginTime, new String [] { pattern }).getTime() / 1000 ;
            if (loginTimeSeconds <= 0) {
                return;
            }
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            return;
        }
        // 提取其它字段
        String accountType = obj.getString("accountType");
        String oauthType = obj.getString("oauthType");
        String addr = obj.getString("addr");
        String appType = obj.getString("appType");
        String appPlatform = obj.getString("appPlatform");
        String appVersion = obj.getString("appVersion");
        int appVersionNumber = -1;
        if (obj.containsField("appVersionNumber")) {
            appVersionNumber = obj.getInt("appVersionNumber");
        }
        String deviceNumber = obj.getString("deviceNumber");
        String imei = obj.getString("imei");
        String operatorInfo = obj.getString("operatorInfo");
        String kernelInfo = obj.getString("kernelInfo");

        // 生成RowKey：12位时间戳(精确到秒，前面补0) + 12位用户ID(左边补零)
        byte [] rowKeyBytes = Bytes.toBytes(String.format("%012d", loginTimeSeconds) + String.format("%012d", userId));
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rowKeyBytes);

        // 输出 family / qualifier ，将写入到HBase
        // 登录用户相关信息 family = user，App相关信息 family = app
        byte [] familyUser = Bytes.toBytes("user");
        byte [] familyApp = Bytes.toBytes("app");

        // 用户相关信息列族
        context.write(rowKey, new KeyValue(rowKeyBytes, familyUser, Bytes.toBytes("userId"), Bytes.toBytes(userId)));
        context.write(rowKey, new KeyValue(rowKeyBytes, familyUser, Bytes.toBytes("loginTime"), Bytes.toBytes(loginTimeSeconds)));
        if (StringUtils.isNotBlank(accountType)) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyUser, Bytes.toBytes("accountType"), Bytes.toBytes(accountType)));
        }
        if (StringUtils.isNotBlank(oauthType)) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyUser, Bytes.toBytes("oauthType"), Bytes.toBytes(oauthType)));
        }
        if (StringUtils.isNotBlank(addr)) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyUser, Bytes.toBytes("addr"), Bytes.toBytes(addr)));
        }

        // App相关信息列族
        if (StringUtils.isNotBlank(appType)) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("appType"), Bytes.toBytes(appType)));
        }
        if (StringUtils.isNotBlank(appPlatform)) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("appPlatform"), Bytes.toBytes(appPlatform)));
        }
        if (StringUtils.isNotBlank(appVersion)) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("appVersion"), Bytes.toBytes(appVersion)));
        }
        if (appVersionNumber > 0) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("appVersionNumber"), Bytes.toBytes(appVersionNumber)));
        }
        if (StringUtils.isNotBlank(deviceNumber)) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("deviceNumber"), Bytes.toBytes(deviceNumber)));
        }
        if (StringUtils.isNotBlank(imei)) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("imei"), Bytes.toBytes(imei)));
        }
        if (StringUtils.isNotBlank(operatorInfo)) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("operatorInfo"), Bytes.toBytes(operatorInfo)));
        }
        if (StringUtils.isNotBlank(kernelInfo)) {
            context.write(rowKey, new KeyValue(rowKeyBytes, familyApp, Bytes.toBytes("kernelInfo"), Bytes.toBytes(kernelInfo)));
        }

    }

}
