package com.zlikun.learning.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-19 10:31
 */
public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable value = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int num = 0;
        for (IntWritable val : values) {
            num += val.get();
        }
        value.set(num);
        context.write(key, value);
    }

}
