package com.zlikun.learning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-07 09:08
 */
public class TokenizerMapper extends Mapper<LongWritable, Text, Text ,IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            word.set(token);
            context.write(word, one);
        }

    }
}
