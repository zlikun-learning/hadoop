package com.zlikun.learning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-07 09:07
 */
public class WordCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[] {
                "hdfs://zlikun:9000/input",
                "hdfs://zlikun:9000/output/03"
        } ;

        System.setProperty("HADOOP_USER_NAME", "zlikun");

        // 如果这里配置了，可以不需要在classpath下放`四个配置文件`，但更建议使用配置文件，会自动加载，打包时候又不会打包进去
        Configuration conf = new Configuration();
//        conf.set("yarn.resourcemanager.hostname", "hadoop.zlikun.com");
//        conf.set("fs.defaultFS", "hdfs://hadoop.zlikun.com:9000");
//        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("mapreduce.app-submission.cross-platform", "true");

        Job job = Job.getInstance(conf);
        job.setJar("E:\\studio\\git\\hadoop\\02-mapreduce\\target\\mr.jar");
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
