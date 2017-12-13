package com.zlikun.learning;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 9.2.4 组合键排序，找出一年中气温最大值
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-13 16:20
 */
public class MaxTemperatureUsingSecondarySort extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args) ;
        if (job == null) return -1;
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setPartitionerClass(FirstPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(IntPair.class);
        job.setOutputValueClass(NullWritable.class);

        // 本地运行时，加上该语句
        job.setJar("E:\\studio\\git\\hadoop\\03-mapreduce-sort\\target\\mr.jar");

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // 测试用参数
        args = new String [] {
                "hdfs://zlikun:9000/ncdc",
                "hdfs://zlikun:9000/output/16"
        } ;
        int exitCode = ToolRunner.run(new MaxTemperatureUsingSecondarySort(), args);
        System.exit(exitCode);
    }

    static class MaxTemperatureMapper extends Mapper<LongWritable, Text, IntPair, NullWritable> {
        private NcdcRecordParser parser = new NcdcRecordParser();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if (parser.isValidTemperature()) {
                // 以年份(整型) + 气温(整型)构成组合键
                context.write(new IntPair(parser.getYearInt(), parser.getAirTemperature()), NullWritable.get());
            }
        }
    }

    static class MaxTemperatureReducer extends Reducer<IntPair, Iterable<NullWritable>, IntPair, NullWritable> {
        @Override
        protected void reduce(IntPair key, Iterable<Iterable<NullWritable>> values, Context context) throws IOException, InterruptedException {
            // 直接输出组件键，值用NULL填充
            context.write(key, NullWritable.get());
        }
    }

    static class FirstPartitioner extends Partitioner<IntPair, NullWritable> {
        @Override
        public int getPartition(IntPair key, NullWritable value, int partitions) {
            // 计算分区逻辑，这里取年份来分区，所以相同年的记录会被分配到同个分区
            return Math.abs(key.getFirst() * 127) % partitions;
        }
    }

    static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(IntPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntPair ip1 = (IntPair) a;
            IntPair ip2 = (IntPair) b;
            // 默认：年份升序
            int cmp = IntPair.compare(ip1.getFirst(), ip2.getFirst());
            if (cmp != 0) return cmp ;
            // 反转：气温降序
            return - IntPair.compare(ip1.getSecond(), ip2.getSecond());
        }
    }

    static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(IntPair.class, true);
        }
        // 分组比较，这里只比较年份
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntPair ip1 = (IntPair) a;
            IntPair ip2 = (IntPair) b;
            return IntPair.compare(ip1.getFirst(), ip2.getFirst());
        }
    }

}
