package com.zlikun.learning.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * 词频统计MR程序
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-19 10:19
 */
public class WordCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[] {
                "hdfs://zlikun:9000/input",
                "hdfs://zlikun:9000/output/20"
        } ;

        // 设置用户名
        System.setProperty("HADOOP_USER_NAME", "zlikun");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word_count");
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        job.setJarByClass(WordCount.class);
        job.setJar(new File("04-example\\target\\mr.jar").getAbsolutePath());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true) ;
        if (status) {
            System.out.println("word_count job is complete .");
        }
        System.exit(status ? 0 : 1);

    }

}
