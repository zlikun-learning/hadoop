package com.zlikun.learning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 9.2.2 部分排序
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-13 14:42
 */
public class SortByTemperatureUsingHashPartitioner extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        // 假设采用5个 reducer 来运行程序，将会输出5个已排序的输出文件
        conf.set("mapreduce.job.reduces", "5");

        Job job = JobBuilder.parseInputAndOutput(this, conf, args) ;
        if (job == null) {
            return -1;
        }

        job.setInputFormatClass(SequenceFileInputFormat.class);

        // 根据输入的记录的键对数据集排序，这里利用IntWritable键对顺序文件排序
        // 键的顺序是由RawComparator控制的
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        job.setJar("E:\\studio\\git\\hadoop\\03-mapreduce-sort\\target\\mr.jar");

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // 测试用参数，用 9.2.1 小节生成的有序文件作为源数据
        args = new String [] {
                "hdfs://zlikun:9000/output/13",
                "hdfs://zlikun:9000/output/14"
        } ;
        int exitCode = ToolRunner.run(new SortByTemperatureUsingHashPartitioner(), args);
        System.exit(exitCode);
    }

}
