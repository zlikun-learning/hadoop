package com.zlikun.learning.dau;

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
public class DauReducer extends Reducer<DateRecord, LongWritable, DateRecord, NullWritable> {

    @Override
    protected void reduce(DateRecord key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        Set<Long> set = new HashSet<>();
        // 值为用户ID列表，计算两个值，一个是总数，一个是重复总数
        for (LongWritable val : values) {
            count ++;
            set.add(val.get());
        }

        key.count = count;
        key.users = set.size();

        context.write(key, NullWritable.get());
    }
}
