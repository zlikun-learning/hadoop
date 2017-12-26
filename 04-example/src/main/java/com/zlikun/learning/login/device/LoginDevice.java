package com.zlikun.learning.login.device;

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
 * 统计各设备登录人次，仅区分：PC、Android、IOS三类设备
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-26 11:17
 */
public class LoginDevice {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length == 0) {
            args = new String[]{
                    "hdfs://zlikun:9000/user/zlikun/NEW_LOGIN_LOG",
                    "hdfs://zlikun:9000/output/48"
            };
        }

        // 设置用户名
        String os = System.getenv("os");
        if (os != null && os.startsWith("Windows")) {
            System.setProperty("HADOOP_USER_NAME", "zlikun");
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "login_device");
        job.setMapperClass(DeviceMapper.class);
        job.setCombinerClass(DeviceReducer.class);
        job.setReducerClass(DeviceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        if (os != null && os.startsWith("Windows")) {
            job.setJar(new File("04-example\\target\\mr.jar").getAbsolutePath());
        } else {
            job.setJarByClass(LoginDevice.class);
        }
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true) ;
        System.exit(status ? 0 : 1);
    }

    public static class DeviceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private LoginLogParser parser = new LoginLogParser();
        private Text device = new Text();
        private IntWritable one = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value.toString());
            if (parser.isValid()) {
                device.set(parser.getClient().toString());
                context.write(device, one);
            }
        }
    }

    public static class DeviceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

}
