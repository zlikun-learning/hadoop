package com.zlikun.learning.logins.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

/**
 * 使用HFile方式导入MySQL数据到HBase中
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-01-30 11:31
 */
public class LoginMapReduceLocal extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
        job.setJar(new File("08-hbase-import\\target\\mr.jar").getAbsolutePath());

        // 配置Mongo数据输入
        MongoConfigUtil.setInputURI(job.getConfiguration(), "mongodb://login_logs:ablejava@192.168.9.223:27017/login_logs.LOGIN_LOG_20171227");
        BasicDBObject query = new BasicDBObject();
        MongoConfigUtil.setQuery(job.getConfiguration(), query);

        // 配置MapReduce程序
        job.setMapperClass(LoginMapper.class);
        job.setReducerClass(LoginReducer.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);

        Path outputPath = new Path("/output/01");

        // 配置输入输出
        job.setInputFormatClass(MongoInputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 配置HBase
        Configuration conf = job.getConfiguration();
        conf.set("hbase.master","m4:16010");
        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("user_login_logs");
        Table table = connection.getTable(tableName);
        RegionLocator regionLocator = connection.getRegionLocator(tableName);
        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);

        boolean flag = job.waitForCompletion(true);

        // load hfile
        Admin admin = connection.getAdmin();
        LoadIncrementalHFiles loadIncrementalHFiles = new LoadIncrementalHFiles(conf);
        loadIncrementalHFiles.doBulkLoad(outputPath, admin, table, regionLocator);
        admin.close();
        regionLocator.close();
        connection.close();

        return flag ? 1 : 0 ;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new LoginMapReduceLocal(), args);
        System.exit(status);
    }

}
