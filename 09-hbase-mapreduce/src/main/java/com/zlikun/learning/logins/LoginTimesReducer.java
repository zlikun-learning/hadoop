package com.zlikun.learning.logins;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 汇总计数
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-01-31 19:19
 */
public class LoginTimesReducer extends Reducer<LongWritable, LongWritable, TblRecord, NullWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0L ;
        for (LongWritable val : values) {
            count ++;   // 记录的是时间戳，不会重复，所以直接自增
        }
        context.write(new TblRecord(key.get(), count), NullWritable.get());
    }

}
