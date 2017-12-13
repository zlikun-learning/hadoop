package com.zlikun.learning;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 9.2.1 准备
 * 将天气数据转换成SequenceFile格式
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-13 14:12
 */
public class SortDataPreprocessor extends Configured implements Tool {

    static class CleanerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private NcdcRecordParser parser = new NcdcRecordParser();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if (parser.isValidTemperature()) {
                context.write(new IntWritable(parser.getAirTemperature()), value);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args) ;
        if (job == null) {
            return -1;
        }

        // 只有Mapper，没有Reducer，使用Mapper过滤无效气温值
        job.setMapperClass(CleanerMapper.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        // 输出块压缩的顺序文件
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        // 本地运行时，加上该语句
        job.setJar("E:\\studio\\git\\hadoop\\03-mapreduce-sort\\target\\mr.jar");

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // 测试用参数
        args = new String [] {
                "hdfs://zlikun:9000/ncdc",
                "hdfs://zlikun:9000/output/13"
        } ;
        int exitCode = ToolRunner.run(new SortDataPreprocessor(), args);
        System.exit(exitCode);
    }

}
