package com.zlikun.learning.max;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-20 09:26
 */
public class TemperatureReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private IntWritable maxTemperature = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int maxValue = Integer.MIN_VALUE;
        for (IntWritable val : values) {
            maxValue = Math.max(maxValue, val.get());
        }
        maxTemperature.set(maxValue);
        context.write(key, maxTemperature);

    }

}
