package com.zlikun.learning;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

/**
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-11-23 11:09
 */
@Slf4j
public class HadoopUrlTest {

    static {
        // 为了让Java程序能识别Hadoop的hdfs URL方案，需要做一些额外工作
        // 下面语句是实现该通力的一种方法，该语句只能在JVM中运行一次，所以一般放静态代码块中
        // 该机制的一个弊端是如果有其它第三方类(库)已经声明了一个URLStreamHandlerFactory实例，将无法再使用该方法从Hadoop中读取数据
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    /**
     * 测试遇到如下问题：
     * org.apache.hadoop.hdfs.BlockMissingException: Could not obtain block: BP-510542202-127.0.1.1-1511414295212:blk_1073741825_1001 file=/user/hadoop/lang.txt
     * @param args
     */
    public static void main(String[] args) {

        InputStream in = null ;
        try {
            in = new URL("hdfs://hadoop.zlikun.com:9000/user/hadoop/lang.txt").openStream() ;
            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }

    }

}
