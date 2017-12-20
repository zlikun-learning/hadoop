package com.zlikun.learning.max;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 映射年份和气温
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-20 09:24
 */
public class TemperatureMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private NcdcRecordParser parser = new NcdcRecordParser();

    private IntWritable year = new IntWritable();
    private IntWritable temperature = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        parser.parse(value);

        if (parser.isValidTemperature()) {
            year.set(parser.getYearInt());
            temperature.set(parser.getAirTemperature());
            context.write(year, temperature);
        }

    }
}
