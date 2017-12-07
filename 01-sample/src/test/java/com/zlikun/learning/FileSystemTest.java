package com.zlikun.learning;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-11-24 10:21
 */
@Slf4j
public class FileSystemTest {

    @Test
    public void get() throws IOException, InterruptedException {

        // 设置用户名
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        final String uri = "hdfs://hadoop.zlikun.com:9000/user/hadoop/input/lang.txt" ;

        Configuration conf = new Configuration() ;
        FileSystem fs = FileSystem.get(URI.create(uri), conf, "hadoop") ;
        InputStream in = null ;
        try {
            in = fs.open(new Path(uri)) ;
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }

    }

    /**
     * 创建文件(将本地文件复制到HDFS上)
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void create() throws IOException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // local file
        InputStream in = new ByteArrayInputStream("nginx tomcat jetty undertow apache iis".getBytes());

        // hdfs file
        final String uri = "hdfs://hadoop.zlikun.com:9000/user/hadoop/input/server.txt" ;
        Configuration conf = new Configuration() ;
        FileSystem fs = FileSystem.get(URI.create(uri), conf, "hadoop") ;
        OutputStream out = fs.create(new Path(uri), () -> {
            System.out.print(".");
        });
        IOUtils.copyBytes(in, out, 4096, true);
    }

    /**
     * 判断目录(文件)是否存在
     * @throws IOException
     */
    @Test
    public void exists() throws IOException {
        Configuration conf = new Configuration() ;
        FileSystem fs = FileSystem.get(conf) ;
        assertTrue(fs.exists(new Path("hdfs://hadoop.zlikun.com:9000/user/hadoop/input")));
        assertTrue(fs.exists(new Path("hdfs://hadoop.zlikun.com:9000/user/hadoop/input/server.txt")));
    }

    /**
     * 列出文件
     */
    @Test
    public void list() throws IOException {
        Configuration conf = new Configuration() ;
        FileSystem fs = FileSystem.get(conf) ;
        FileStatus [] list = fs.listStatus(new Path("hdfs://hadoop.zlikun.com:9000/user/hadoop/input"));
        assertEquals(2, Arrays.stream(list).count());
        Arrays.stream(list).forEach(file -> System.out.println(file.getPath().toString()));
    }

    /**
     * 创建目录
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void mkdirs() throws IOException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration conf = new Configuration() ;
        FileSystem fs = FileSystem.get(conf) ;
        assertTrue(fs.mkdirs(new Path("hdfs://hadoop.zlikun.com:9000/user/hadoop/books")));
    }

}
