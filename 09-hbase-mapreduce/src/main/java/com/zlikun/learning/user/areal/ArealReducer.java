package com.zlikun.learning.user.areal;

import com.zlikun.learning.dau.DateRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-09 10:42
 */
public class ArealReducer extends Reducer<ArealRecord, LongWritable, ArealRecord, NullWritable> {

    @Override
    protected void reduce(ArealRecord key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        // 计算用户数(去重)
        Set<Long> set = new HashSet<>();
        for (LongWritable val : values) {
            set.add(val.get());
        }
        key.users = set.size();
        context.write(key, NullWritable.get());
    }

}
