package com.zlikun.learning.login.times;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-26 11:36
 */
public class LoginTimesTest {

    @Test
    public void map() throws IOException {
        Text value = new Text("8077,162047547,ANDROID_STUDENT_STORE,2016-10-25,2016-10-25 17:37:04.0");
        MapDriver.newMapDriver(new LoginTimes.TimesMapper())
                .withInput(new LongWritable(0), value)
                .withOutput(new LongWritable(162047547), new IntWritable(1))
                .runTest();

    }

    @Test
    public void reduce() throws IOException {
        ReduceDriver.newReduceDriver(new LoginTimes.TimesReducer())
                .withInput(new LongWritable(162047547), Arrays.asList(new IntWritable(1), new IntWritable(2)))
                .withOutput(new LongWritable(162047547), new IntWritable(3))
                .runTest();
    }

}