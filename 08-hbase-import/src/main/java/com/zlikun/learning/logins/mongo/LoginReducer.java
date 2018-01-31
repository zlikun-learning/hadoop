package com.zlikun.learning.logins.mongo;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * RowKey是使用登录时间戳 + 登录用户ID设计的，实际有重复的可能性，这里取第一条，即：一秒内只能有一条登录日志
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-01-30 18:28
 */
public class LoginReducer extends Reducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, KeyValue> {

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<KeyValue> values, Context context) throws IOException, InterruptedException {
        Iterator<KeyValue> iter = values.iterator();
        // 只迭代一次
        if (iter.hasNext()) {
            context.write(key, iter.next());
        }
    }
}
