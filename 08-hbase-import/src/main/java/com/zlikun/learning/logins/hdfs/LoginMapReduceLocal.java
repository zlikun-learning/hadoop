package com.zlikun.learning.logins.hdfs;

import com.zlikun.learning.logins.TblRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-01-30 11:31
 */
public class LoginMapReduceLocal extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
        job.setJar(new File("08-hbase-import\\target\\mr.jar").getAbsolutePath());
        // 配置MapReduce程序
        job.setMapperClass(LoginMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // job.setInputFormatClass(DBInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("hdfs://m4:9000/output/01"));
        // 添加mysql驱动依赖(避免直接在集群中添加，要重启集群，不方便)
        // 下面语句只能集群中运行mapreduce程序时用，本地执行会报错，另一种方法两者可以兼顾，即：通过打包将依赖JAR一并打包实现
        // job.addFileToClassPath(new Path("/lib/mysql/mysql-connector-java-5.1.45.jar"));
        DBConfiguration.configureDB(job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.9.223:3306/USER_LOG_LOGIN",
                "root",
                "ablejava");
        // 截止 NEW_LOGIN_LOG_2017_06_18 表查询字段列表
        String[] fields = {"USER_ID", "ACCOUNT_TYPE", "OPEN_TYPE", "CLIENT_CODE", "APP_VERSION", "ADDR", "LOGIN_TIME"};
        // 从 NEW_LOGIN_LOG_2017_06_25 开始，截止NEW_LOGIN_LOG_2017_12_17，增加：DEVICE_NUMBER、IMEI两个字段
        // String[] fields = {"USER_ID", "ACCOUNT_TYPE", "OPEN_TYPE", "CLIENT_CODE", "APP_VERSION", "ADDR", "LOGIN_TIME", "DEVICE_NUMBER", "IMEI"};
        // DBInputFormat.setInput() 隐含了：job.setInputFormatClass(DBInputFormat.class);
        DBInputFormat.setInput(job, TblRecord.class,
                "NEW_LOGIN_LOG", null, null, fields);
        return job.waitForCompletion(true) ? 1 : 0 ;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new LoginMapReduceLocal(), args);
        System.exit(status);
    }

}
