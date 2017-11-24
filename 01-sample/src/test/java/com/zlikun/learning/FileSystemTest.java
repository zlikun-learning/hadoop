package com.zlikun.learning;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-11-24 10:21
 */
@Slf4j
public class FileSystemTest {

    /**
     * 又TM的遇到这个问题了，后面再搞吧
     * org.apache.hadoop.hdfs.BlockMissingException: Could not obtain block: BP-510542202-127.0.1.1-1511414295212:blk_1073741825_1001 file=/user/hadoop/lang.txt
     * @throws IOException
     */
    @Test
    public void test() throws IOException, InterruptedException {

        final String uri = "hdfs://hadoop.zlikun.com:9000/user/hadoop/lang.txt" ;

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

}
