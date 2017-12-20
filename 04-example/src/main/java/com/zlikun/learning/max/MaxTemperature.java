package com.zlikun.learning.max;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * 统计最大气温值
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-20 09:17
 */
public class MaxTemperature {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length == 0) {
            args = new String[]{
                    "hdfs://zlikun:9000/ncdc",
                    "hdfs://zlikun:9000/output/33"
            };
        }

        // 设置用户名
        String os = System.getenv("os");
        if (os != null && os.startsWith("Windows")) {
            System.setProperty("HADOOP_USER_NAME", "zlikun");
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "max_temperature");
        job.setMapperClass(TemperatureMapper.class);
        job.setCombinerClass(TemperatureReducer.class);
        job.setReducerClass(TemperatureReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        if (os != null && os.startsWith("Windows")) {
            job.setJar(new File("04-example\\target\\mr.jar").getAbsolutePath());
        } else {
            job.setJarByClass(MaxTemperature.class);
        }
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true) ;
        if (status) {
            System.out.println("word_count job is complete .");
        }
        System.exit(status ? 0 : 1);

    }

}
