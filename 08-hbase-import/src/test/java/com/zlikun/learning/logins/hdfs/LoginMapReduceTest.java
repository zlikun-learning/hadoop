package com.zlikun.learning.logins.hdfs;

import com.zlikun.learning.logins.TblRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-01-30 13:38
 */
public class LoginMapReduceTest {

    @Test
    public void map() throws IOException {
        TblRecord value = new TblRecord();
        value.userId = 1L;
        value.loginTime = 10000L ;
        MapDriver.newMapDriver(new LoginMapper())
                .withInput(new LongWritable(1), value)
                .withOutput(new Text("1,,,10000"), NullWritable.get())
                .runTest();

    }

    @Test
    public void reduce() throws IOException {
//        ReduceDriver.newReduceDriver(new LoginTimes.TimesReducer())
//                .withInput(new LongWritable(162047547), Arrays.asList(new IntWritable(1), new IntWritable(2)))
//                .withOutput(new LongWritable(162047547), new IntWritable(3))
//                .runTest();
    }

}