package com.zlikun.learning.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * 统计 Top 10 单词列表，本例将使用 WordCount 任务输出的数据为数据源
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-19 10:56
 */
public class TopN {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length == 0) {
            args = new String[]{
                    "hdfs://zlikun:9000/output/20",
                    "hdfs://zlikun:9000/output/32"
            };
        }

        // 设置用户名
        String os = System.getenv("os");
        if (os != null && os.startsWith("Windows")) {
            System.setProperty("HADOOP_USER_NAME", "zlikun");
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "top_n");
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
        // 下面两句解决：Type mismatch in key from map: expected org.apache.hadoop.io.Text, received ... 问题
        job.setMapOutputKeyClass(NewKey.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        if (os != null && os.startsWith("Windows")) {
            job.setJar(new File("04-example\\target\\mr.jar").getAbsolutePath());
        } else {
            job.setJarByClass(TopN.class);
        }
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true) ;
        if (status) {
            System.out.println("top_n job is complete .");
        }
        System.exit(status ? 0 : 1);

    }

}
