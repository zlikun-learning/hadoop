package com.zlikun.learning.mysql.logins;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 登录日期mapreduce程序，输出$userId + $date到HDFS中，以供后续使用
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-01-16 11:39
 */
public class LoginDaysMapReduce extends Configured implements Tool {

    /**
     * 将数据库中数据映射为：$userId + $date(与1970的相关天数) 格式
     */
    public static class LoginDaysMapper extends Mapper<LongWritable, TblRecord, Text, NullWritable> {

        @Override
        protected void map(LongWritable key, TblRecord value, Context context) throws IOException, InterruptedException {
            if (value.days > 0) {
                context.write(new Text(value.toString()), NullWritable.get());
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
        job.setJarByClass(LoginDaysMapReduce.class);
        // 配置MapReduce程序
        job.setMapperClass(LoginDaysMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setInputFormatClass(DBInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("hdfs://zlikun:9000/login_days/example"));
        // 添加mysql驱动依赖(避免直接在集群中添加，要重启集群，不方便)
        // 下面语句只能集群中运行mapreduce程序时用，本地执行会报错，另一种方法两者可以兼顾，即：通过打包将依赖JAR一并打包实现
        // job.addArchiveToClassPath(new Path("hdfs://zlikun:9000/lib/mysql/mysql-connector-java-5.1.45.jar"));
        job.addArchiveToClassPath(new Path("/lib/mysql/mysql-connector-java-5.1.45.jar"));
        DBConfiguration.configureDB(job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.9.223:3306/USER_LOG_LOGIN",
                "root",
                "ablejava");
        String [] fields = {"USER_ID", "LOGIN_TIME"};
        DBInputFormat.setInput(job, TblRecord.class,
                "NEW_LOGIN_LOG", null, null, fields);
        return job.waitForCompletion(true) ? 1 : 0 ;
    }

    /**
     * args[0]  jdbcUrl
     * args[1]  username
     * args[2]  password
     * args[3]  tableName
     * args[4]  outputPath
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new LoginDaysMapReduce(), args);
        System.exit(status);
    }

}
