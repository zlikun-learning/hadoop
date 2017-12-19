package com.zlikun.learning.top;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-19 13:39
 */
public class TopNReducer extends Reducer<NewKey, NullWritable, Text, IntWritable> {

    private Text word = new Text();
    private IntWritable count = new IntWritable();

    @Override
    protected void reduce(NewKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        word.set(key.getKey());
        count.set(key.getValue());
        context.write(word, count);
    }

}
