package com.zlikun.learning.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * WordCount 单元测试
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-19 10:21
 */
public class WordCountTest {

    @Test
    public void map() throws IOException {
        Text value = new Text("nginx jetty nginx tomcat");
        MapDriver.newMapDriver(new TokenizerMapper())
                .withInput(new LongWritable(0), value)
                .withOutput(new Text("nginx"), new IntWritable(1))
                .withOutput(new Text("jetty"), new IntWritable(1))
                .withOutput(new Text("nginx"), new IntWritable(1))
                .withOutput(new Text("tomcat"), new IntWritable(1))
                .runTest();

    }

    @Test
    public void reduce() throws IOException {
        ReduceDriver.newReduceDriver(new IntSumReducer())
                .withInput(new Text("nginx"), Arrays.asList(new IntWritable(1), new IntWritable(2)))
                .withOutput(new Text("nginx"), new IntWritable(3))
                .runTest();
    }
}