package com.zlikun.learning.max_single;

import com.zlikun.learning.max.NcdcRecordParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * 获取全部气温数据中的最大值、最小值
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-20 10:04
 */
public class SingleMaxTemperature {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length == 0) {
            args = new String[]{
                    "hdfs://zlikun:9000/ncdc",
                    "hdfs://zlikun:9000/output/36"
            };
        }

        // 设置用户名
        String os = System.getenv("os");
        if (os != null && os.startsWith("Windows")) {
            System.setProperty("HADOOP_USER_NAME", "zlikun");
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "max_temperature");
        job.setMapperClass(MaxMapper.class);
        job.setReducerClass(MaxReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        if (os != null && os.startsWith("Windows")) {
            job.setJar(new File("04-example\\target\\mr.jar").getAbsolutePath());
        } else {
            job.setJarByClass(SingleMaxTemperature.class);
        }
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true) ;

    }

    public static class MaxMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private NcdcRecordParser parser = new NcdcRecordParser();
        private IntWritable number = new IntWritable();
        private int maxTemperature = Integer.MIN_VALUE;
        private int minTemperature = Integer.MAX_VALUE;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if (parser.isValidTemperature()) {
                // 记录最大气温值，这里简化问题，不记录最大气温值对应的年份(如实现，需要使用组合键)
                maxTemperature = Math.max(maxTemperature, parser.getAirTemperature());
                // 引申，记录最小值
                minTemperature = Math.min(minTemperature, parser.getAirTemperature());
            }
        }

        /**
         * #cleanup() 方法只执行一次
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new IntWritable(maxTemperature), new IntWritable(minTemperature));
        }
    }

    public static class MaxReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        int maxTemperature = Integer.MIN_VALUE;
        private int minTemperature = Integer.MAX_VALUE;
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            maxTemperature = Math.max(maxTemperature, key.get());
            for (IntWritable val : values) {
                minTemperature = Math.min(minTemperature, val.get());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new IntWritable(maxTemperature), new IntWritable(minTemperature));
        }
    }

}
