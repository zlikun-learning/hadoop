package com.zlikun.learning.avg;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-20 09:38
 */
public class TemperatureReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private IntWritable avg = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 1;
        int total = 0;
        for (IntWritable val : values) {
            count ++;
            total += val.get();
        }
        avg.set(total / count);
        context.write(key, avg);
    }

}
