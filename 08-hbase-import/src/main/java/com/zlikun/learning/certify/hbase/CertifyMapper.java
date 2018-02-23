package com.zlikun.learning.certify.hbase;

import com.zlikun.learning.certify.TblRecord;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-23 10:23
 */
public class CertifyMapper extends Mapper<LongWritable, TblRecord, ImmutableBytesWritable, KeyValue> {

    @Override
    protected void map(LongWritable key, TblRecord value, Context context) throws IOException, InterruptedException {
        if (value == null || !value.isValid()) return;
        // 生成RowKey：12位秒级时间戳(左边补零) + 12位用户ID(左边补零)，共24个字节
        byte [] rowKeyBytes = Bytes.toBytes(new StringBuilder().append(String.format("%012d", value.createTime)).append(String.format("%012d", value.userId)).toString());
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rowKeyBytes);

        byte [] family = Bytes.toBytes("i");    // 表示info

        context.write(rowKey, new KeyValue(rowKeyBytes, family, Bytes.toBytes("id"), Bytes.toBytes(value.id)));
        context.write(rowKey, new KeyValue(rowKeyBytes, family, Bytes.toBytes("userId"), Bytes.toBytes(value.userId)));
        context.write(rowKey, new KeyValue(rowKeyBytes, family, Bytes.toBytes("schoolId"), Bytes.toBytes(value.schoolId)));
        context.write(rowKey, new KeyValue(rowKeyBytes, family, Bytes.toBytes("role"), Bytes.toBytes(value.role)));
        context.write(rowKey, new KeyValue(rowKeyBytes, family, Bytes.toBytes("createTime"), Bytes.toBytes(value.createTime)));

        // 使用分布式缓存，填充学校名称、学校所属城市、学校所属省份，以方便后续计算任务
        // 如果这样存储，应该需要创建二级索引来提升计算性能(也可以通过MapReduce任务来实现)

    }
}
