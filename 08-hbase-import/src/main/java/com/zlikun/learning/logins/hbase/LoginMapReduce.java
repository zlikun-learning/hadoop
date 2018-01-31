package com.zlikun.learning.logins.hbase;

import com.zlikun.learning.logins.TblRecord;
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
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 执行时需要把HBase加入到CLASSPATH中
 * HADOOP_CLASSPATH=`$HBASE_HOME/bin/hbase classpath` $HADOOP_HOME/bin/hadoop jar mr.jar com.zlikun.learning.logins.hbase.LoginMapReduce
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
        // 配置MapReduce程序
        job.setMapperClass(LoginMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setReducerClass(LoginReducer.class);

        // 配置输入
        job.addFileToClassPath(new Path("/lib/mysql/mysql-connector-java-5.1.45.jar"));
        DBConfiguration.configureDB(job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                args[0],
                args[1],
                args[2]);
        // 截止 NEW_LOGIN_LOG_2017_06_18 表查询字段列表
        String[] fields = {"USER_ID", "ACCOUNT_TYPE", "OPEN_TYPE", "CLIENT_CODE", "APP_VERSION", "ADDR", "LOGIN_TIME"};
        // 从 NEW_LOGIN_LOG_2017_06_25 开始，截止NEW_LOGIN_LOG_2017_12_17，增加：DEVICE_NUMBER、IMEI两个字段
        // String[] fields = {"USER_ID", "ACCOUNT_TYPE", "OPEN_TYPE", "CLIENT_CODE", "APP_VERSION", "ADDR", "LOGIN_TIME", "DEVICE_NUMBER", "IMEI"};

        // set input / output file path
        // DBInputFormat.setInput() 隐含了：job.setInputFormatClass(DBInputFormat.class);
        DBInputFormat.setInput(job, TblRecord.class,
                args[3], null, null, fields);
        Path outputPath = new Path("/tmp/mysql/" + args[3]);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 配置HBase
        Configuration conf = job.getConfiguration();
        // 设置master连接地址
        conf.set("hbase.master", args[4]);
//        // 设置连接参数：HBase数据库所在的主机IP
//        conf.set("hbase.zookeeper.quorum", "m4");
//        // 设置连接参数：HBase数据库使用的端口
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("user_login_logs");
        Table table = connection.getTable(tableName);
        RegionLocator regionLocator = connection.getRegionLocator(tableName);
        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);

//        // 配置 KeyValue 类型序列化，否则会报如下错误：
//        // Error: java.io.IOException: Initialization of all the collectors failed. Error in last collector was :null
//        conf.setStrings("io.serializations",
//                conf.get("io.serializations"),
//                KeyValueSerialization.class.getName());

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

    /**
     * 传入参数说明：
     * 1、jdbcUrl
     * 2、username
     * 3、password
     * 4、tableName
     * 5、HMaster节点地址(host:port)
     *
     * HADOOP_CLASSPATH=`$HBASE_HOME/bin/hbase classpath` $HADOOP_HOME/bin/hadoop jar mr.jar com.zlikun.learning.logins.hbase.LoginMapReduce jdbc:mysql://192.168.9.223:3306/test root ablejava NEW_LOGIN_LOG m4:16010
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: com.zlikun.learning.logins.hbase.LoginMapReduce <jdbcUrl> <username> <password> <tableName> <hMasterSever>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new LoginMapReduce(), args);
        System.exit(status);
    }

}
