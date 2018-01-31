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

/**
 * 执行时需要把HBase加入到CLASSPATH中
 * HADOOP_CLASSPATH=`$HBASE_HOME/bin/hbase classpath`:mongo-java-driver-3.2.1.jar $HADOOP_HOME/bin/hadoop jar mr-bin.jar com.zlikun.learning.logins.mongo.LoginMapReduce
 *
 * 事先把表在HBase中建出来，并使用`user`和`app`两个列族
 * create 'user_login_logs', 'user', 'app'
 * 0 row(s) in 2.2760 seconds
 * => Hbase::Table - user_login_logs
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-01-30 11:31
 */
public class LoginMapReduce extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        // 配置Mongo数据输入
        MongoConfigUtil.setInputURI(job.getConfiguration(), args[0] + "." + args[1]);
        BasicDBObject query = new BasicDBObject();
        MongoConfigUtil.setQuery(job.getConfiguration(), query);

        // 配置MapReduce程序
        job.setMapperClass(LoginMapper.class);
        job.setReducerClass(LoginReducer.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);

        Path outputPath = new Path("/tmp/mongo/" + args[1]);

        // 配置输入输出
        job.setInputFormatClass(MongoInputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 配置HBase
        Configuration conf = job.getConfiguration();
        conf.set("hbase.master", args[2]);
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

        return flag ? 1 : 0;
    }

    /**
     * 传入参数说明：
     * 1、mongoUri
     * 2、collectionName
     * 3、HMaster节点地址(host:port)
     *
     * HADOOP_CLASSPATH=`$HBASE_HOME/bin/hbase classpath`:mongo-java-driver-3.2.1.jar $HADOOP_HOME/bin/hadoop jar mr-bin.jar com.zlikun.learning.logins.mongo.LoginMapReduce mongodb://login_logs:ablejava@192.168.9.223:27017/login_logs LOGIN_LOG_20171227 m4:16010
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: com.zlikun.learning.logins.mongo.LoginMapReduce <mongoUri> <collectionName> <hMasterSever>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new LoginMapReduce(), args);
        System.exit(status);
    }

}
