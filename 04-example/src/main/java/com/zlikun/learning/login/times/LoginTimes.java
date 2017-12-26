package com.zlikun.learning.login.times;

import com.zlikun.learning.login.LoginLogParser;
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
 * 登录人次统计
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-26 11:15
 */
public class LoginTimes {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length == 0) {
            args = new String[]{
                    "hdfs://zlikun:9000/user/zlikun/NEW_LOGIN_LOG",
                    "hdfs://zlikun:9000/output/47"
            };
        }

        // 设置用户名
        String os = System.getenv("os");
        if (os != null && os.startsWith("Windows")) {
            System.setProperty("HADOOP_USER_NAME", "zlikun");
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "login_times");
        job.setMapperClass(TimesMapper.class);
        job.setCombinerClass(TimesReducer.class);
        job.setReducerClass(TimesReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
        if (os != null && os.startsWith("Windows")) {
            job.setJar(new File("04-example\\target\\mr.jar").getAbsolutePath());
        } else {
            job.setJarByClass(LoginTimes.class);
        }
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true) ;
        System.exit(status ? 0 : 1);

    }

    public static class TimesMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
        private LoginLogParser parser = new LoginLogParser();
        private static final IntWritable one = new IntWritable(1);
        private LongWritable user = new LongWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value.toString());
            if (parser.isValid()) {
                user.set(parser.getUserId());
                context.write(user, one);
            }
        }
    }

    public static class TimesReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
        private IntWritable user = new IntWritable();
        private int times = 0;
        private int users = 0;
        @Override
        protected void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0 ;
            for (IntWritable val : values) {
                sum += val.get();
                times += val.get();
            }
            users ++;
            user.set(sum);
            context.write(key, user);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 这里输出的日志在管理界面中查看 history > reduce > logs 中可以看到
            // 这里不是一个正常的输出方式，如果目的就是为了统计这两个值，可以直接输出到HDFS中
            System.out.println("登录总人数：" + users + ", 总次数：" + times);
        }
    }

}
