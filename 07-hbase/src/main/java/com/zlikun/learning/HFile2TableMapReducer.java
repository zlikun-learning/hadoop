package com.zlikun.learning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 导入HDFS数据到HBase
 *
 HADOOP_CLASSPATH=`$HBASE_HOME/bin/hbase classpath` \
 $HADOOP_HOME/bin/hadoop jar \
 /root/jars/07-hbase-1.0.0.jar com.zlikun.learning.HFile2TableMapReducer \
 user /example/hbase_tsv_user /example/hfile_user
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/1/14 19:27
 */
public class HFile2TableMapReducer extends Configured implements Tool {

    public static class MyHFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] words = value.toString().split("\t");
            byte [] bytes = Bytes.toBytes(words[0]);
            context.write(new ImmutableBytesWritable(bytes),
                    new KeyValue(bytes, Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(words[1])));
            context.write(new ImmutableBytesWritable(bytes),
                    new KeyValue(bytes, Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(words[2])));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // create job
        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
        // set run class
        job.setJarByClass(this.getClass());
        // set input / output
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        // set mapper
        job.setMapperClass(MyHFileMapper.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);
        // set reducer
        job.setReducerClass(PutSortReducer.class);

        Connection connection = ConnectionFactory.createConnection(this.getConf());
        Table table = connection.getTable(TableName.valueOf(args[0]));
        RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(args[0]));
        // set hfile output
        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);

        if (!job.waitForCompletion(true)) {
            throw new IOException("error with job !");
        }

        // load hfile
        Admin admin = connection.getAdmin();
        LoadIncrementalHFiles loadIncrementalHFiles = new LoadIncrementalHFiles(this.getConf());
        loadIncrementalHFiles.doBulkLoad(new Path(args[2]), admin, table, regionLocator);
        admin.close();
        regionLocator.close();
        connection.close();

        return 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        int status = ToolRunner.run(configuration, new HFile2TableMapReducer(), args);
        System.exit(status);
    }

}
