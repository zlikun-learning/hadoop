package com.zlikun.learning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * 针对气温数据排序 《Hadoop权威指南》 第四版 9.2 节
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-13 11:54
 */
public class SortByTemperatureUsingTotalOrderPartitioner extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);

        if (job == null) {
            return -1;
        }

        // 作业只包含Mapper，过滤输入数据并输出一个块压缩的顺序文件
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        // 使用 TotalOrderPartitioner 为排序作业创建分区
        job.setPartitionerClass(TotalOrderPartitioner.class);

        // 设置采样器，用于实现较为平均的分区，参数意义：采样率0.1、最大样本1000、最大分区10
        InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<>(
                0.1, 1000, 10);
        InputSampler.writePartitionFile(job, sampler);

        // InputSampler需要将所写的分区文件加到分布式缓存中
        // Add to DistributedCache
        Configuration conf = job.getConfiguration();
        String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
        URI partitionUri = new URI(partitionFile);
        job.addCacheFile(partitionUri);

        // 本地运行时，加上该语句
        job.setJar("E:\\studio\\git\\hadoop\\03-mapreduce-sort\\target\\mr.jar");

        return job.waitForCompletion(true) ? 0 : 1 ;
    }

    public static void main(String[] args) throws Exception {
        // 测试用参数
        args = new String [] {
                "hdfs://zlikun:9000/output/13",
                "hdfs://zlikun:9000/output/15"
        } ;
        int exitCode = ToolRunner.run(new SortByTemperatureUsingTotalOrderPartitioner(), args);
        System.exit(exitCode);
    }

}
