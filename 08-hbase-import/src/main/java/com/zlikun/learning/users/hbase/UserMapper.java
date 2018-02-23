package com.zlikun.learning.users.hbase;

import com.zlikun.learning.users.TblRecord;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-22 16:57
 */
public class UserMapper extends Mapper<LongWritable, TblRecord, ImmutableBytesWritable, KeyValue> {

    @Override
    protected void map(LongWritable key, TblRecord value, Context context) throws IOException, InterruptedException {
        if (value == null || value.userId == null || value.joinDate == null) return;

        // 生成RowKey：8位日期 + 12位用户ID(左边补零)，共20个字节：20180221000000010086
        byte [] rowKeyBytes = Bytes.toBytes(new StringBuilder().append(value.joinDate).append(String.format("%012d", value.userId)).toString());
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rowKeyBytes);

        byte [] familyUser = Bytes.toBytes("i");    // 表示info，列族使用简写节省空间

        // 用户相关信息列族
        context.write(rowKey, new KeyValue(rowKeyBytes, familyUser, Bytes.toBytes("userId"), Bytes.toBytes(value.userId)));
        context.write(rowKey, new KeyValue(rowKeyBytes, familyUser, Bytes.toBytes("joinDate"), Bytes.toBytes(value.joinDate)));

    }

}
