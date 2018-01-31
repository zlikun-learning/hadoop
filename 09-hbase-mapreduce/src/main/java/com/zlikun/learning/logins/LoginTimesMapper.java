package com.zlikun.learning.logins;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-01-31 19:17
 */
public class LoginTimesMapper extends TableMapper<LongWritable, LongWritable> {

    /**
     * 这里统计用户登录次数，所只需要RowKey即可(RowKey = timestamp + userId 构成，各有12位，左边填0)
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String rowKey = Bytes.toString(key.get());
        if (rowKey.length() != 24) return;
        // 输出 userId => timestamp
        context.write(new LongWritable(NumberUtils.toLong(rowKey.substring(12, 24))), new LongWritable(NumberUtils.toLong(rowKey.substring(0, 12))));
    }

}
