package com.zlikun.learning.counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-12 19:04
 */
public class MapReduceCounter {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[] {
                "hdfs://zlikun:9000/input",
                "hdfs://zlikun:9000/output/07"
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
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(0);   // 只有Mapper没有Reducer

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 输出一个块压缩的顺序文件
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        // 获取计数值
        long nginxCount = job.getCounters().findCounter(Words.NGINX).getValue() ;
        System.out.printf("word 'nginx' appear %d times.", nginxCount);

    }

}
