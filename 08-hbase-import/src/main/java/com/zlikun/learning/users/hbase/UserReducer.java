package com.zlikun.learning.users.hbase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-22 17:02
 */
public class UserReducer extends Reducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, KeyValue> {

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<KeyValue> values, Context context) throws IOException, InterruptedException {
        Iterator<KeyValue> iter = values.iterator();
        if (iter.hasNext()) {
            context.write(key, iter.next());
        }
    }
}
