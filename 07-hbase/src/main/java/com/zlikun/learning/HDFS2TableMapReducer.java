package com.zlikun.learning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 导入HDFS数据到HBase
 *
 HADOOP_CLASSPATH=`$HBASE_HOME/bin/hbase classpath` \
 $HADOOP_HOME/bin/hadoop jar \
 /root/jars/07-hbase-1.0.0.jar com.zlikun.learning.HDFS2TableMapReducer \
 /example/hbase_tsv_user
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/1/14 19:27
 */
public class HDFS2TableMapReducer extends Configured implements Tool {

    public static class MyHdfsMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        private ImmutableBytesWritable rowkey = new ImmutableBytesWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] words = value.toString().split("\t");
            rowkey.set(Bytes.toBytes(words[0]));
            Put put = new Put(Bytes.toBytes(words[0]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(words[1]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(words[2]));
            context.write(rowkey, put);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // create job
        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
        // set run class
        job.setJarByClass(this.getClass());
        // set path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        // set mapper
        job.setMapperClass(MyHdfsMapper.class);
        // set reducer
        TableMapReduceUtil.initTableReducerJob("user", null, job);
        // 不需要Reducer
        job.setNumReduceTasks(0);

        if (!job.waitForCompletion(true)) {
            System.err.println("error with job !");
            return 0;
        }
        return 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        int status = ToolRunner.run(configuration, new HDFS2TableMapReducer(), args);
        System.exit(status);
    }

}
