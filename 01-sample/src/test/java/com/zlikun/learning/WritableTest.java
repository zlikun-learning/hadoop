package com.zlikun.learning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.StringUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Hadoop序列化机制测试
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017/11/25 11:49
 */
public class WritableTest {

    @Test @Ignore
    public void serialization() throws IOException {

        // 使用整型数据类型测试
        Writable writable = new IntWritable(128);

        // 将其序列化为字节数组的过程
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        writable.write(dataOut);
        dataOut.close();
        byte[] data = out.toByteArray();

        // 执行断言，检查序列化的结果是否正确
        assertThat(data.length, is(4));
        assertThat(StringUtils.byteToHexString(data), is("00000080"));
    }

    @Test @Ignore
    public void deserialization() throws IOException {

        // 假设序列化后的数据
        byte[] data = StringUtils.hexStringToByte("00000010");

        // 将其反序列化
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        DataInputStream dataIn = new DataInputStream(in);
        IntWritable writable = new IntWritable();
        writable.readFields(dataIn);
        dataIn.close();

        // 执行断言，检查反序列化结果是否正确
        assertThat(writable.get(), is(16));

    }

    @Test @Ignore
    public void compare() {
        RawComparator<IntWritable> comparator = WritableComparator.get(IntWritable.class);

        IntWritable i1 = new IntWritable(127);
        IntWritable i2 = new IntWritable(32);

        // RawComparator 用于比较两个 Writable 对象，也可以比较两者序列化后的结果
        assertThat(comparator.compare(i1, i2), greaterThan(0));
    }

}
