package com.zlikun.learning.dau;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.Date;

/**
 * 从HBase中读取数据
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-09 09:23
 */
public class DauMapper extends TableMapper<DateRecord, LongWritable> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String rowKey = Bytes.toString(key.get());
        // 获取用户ID及登录时间
        Long timestamp = NumberUtils.toLong(StringUtils.substring(rowKey, 0 , 12));
        Long userId = NumberUtils.toLong(StringUtils.substring(rowKey, 12));
        if (timestamp == null || timestamp <= 0 || userId == null || userId <= 0) return;
        // 获取AppType
        String appType = Bytes.toString(value.getValue(Bytes.toBytes("app"), Bytes.toBytes("appType")));
        if (appType == null) appType = "WEB";
        if (appType.startsWith("ZD_")) appType = "ZD";
        else if (appType.startsWith("JSQ_")) appType = "JSQ";
        else if (appType.startsWith("SX_")) appType = "SX";
        else appType = "WEB";   // 其它一律算作WEB登录

        String date = DateFormatUtils.format(new Date(timestamp * 1000), "yyyy-MM-dd");
        // 统计终端登录人数
        context.write(new DateRecord(date, appType), new LongWritable(userId));
        // 统计全局登录人数
        context.write(new DateRecord(date, "ALL"), new LongWritable(userId));
    }
}
