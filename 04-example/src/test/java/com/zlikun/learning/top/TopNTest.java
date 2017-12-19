package com.zlikun.learning.top;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-19 13:51
 */
public class TopNTest {

    @Test
    public void map() throws IOException {
        Text value = new Text("nginx 1");
        MapDriver.newMapDriver(new TopNMapper())
                .withInput(new LongWritable(0), value)
                .withOutput(new NewKey("nginx", 1), NullWritable.get())
                .runTest();
    }

    @Test
    public void reduce() throws IOException {
        ReduceDriver.newReduceDriver(new TopNReducer())
                .withInput(new NewKey("nginx", 3), Arrays.asList(NullWritable.get()))
                .withOutput(new Text("nginx"), new IntWritable(3))
                .runTest();
    }

}