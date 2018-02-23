package com.zlikun.learning.user.areal;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * 注册用户按省份分布统计
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-23 11:09
 */
public class ArealMapper extends TableMapper<ArealRecord, LongWritable> {

    /**
     * 学校所属省份缓存
     */
    private Map<Integer, String> storage = new HashMap<>();

    /**
     * 使用分布式缓存机制，加载学校 -> 省份对应关系数据
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        hdfs(context);
    }

    private void hdfs(Context context) throws IOException {
        FileSystem fs = FileSystem.get(context.getConfiguration()) ;
        FileStatus[] statuses = fs.listStatus(new Path("/basic/province"));
        for (FileStatus status : statuses) {
            if (!status.isFile()) continue;
            InputStream in = null ;
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte [] buf = null;
            try {
                in = fs.open(status.getPath()) ;
                IOUtils.copyBytes(in, out, 4096, false);
                buf = out.toByteArray();
            } finally {
                IOUtils.closeStream(out);
                IOUtils.closeStream(in);
            }
            if (buf != null) {
                for (String line : new String(buf, "UTF-8").split("\\s+")) {
                    String schoolId = StringUtils.substringBefore(line, ",");
                    String province = StringUtils.substringAfter(line, ",");
                    if (schoolId == null || province == null) continue;
                    storage.put(NumberUtils.toInt(schoolId), province);
                }
            }
        }
    }

    /**
     * 遍历HBase中表数据，根据学校计算用户所属省份，输出：省份、角色信息
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        final byte [] family = Bytes.toBytes("i");
        int schoolId = Bytes.toInt(value.getValue(family, Bytes.toBytes("schoolId")));
        long userId = Bytes.toLong(value.getValue(family, Bytes.toBytes("userId")));
        int role = Bytes.toInt(value.getValue(family, Bytes.toBytes("role")));
        String province = this.storage.get(schoolId);
        if (province == null) province = "其它";
        context.write(new ArealRecord(province, role), new LongWritable(userId));
    }

}
