package com.zlikun.learning.user.areal;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
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
        if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
            for (URI uri : context.getCacheFiles()) {
                System.out.println("URI : " + uri.toString());
                List<String> lines = FileUtils.readLines(new File(uri));
                for (String line : lines) {
                    String schoolId = StringUtils.substringBefore(line, ",");
                    String province = StringUtils.substringAfter(line, ",");
                    if (schoolId == null || province == null) continue;
                    storage.put(NumberUtils.toInt(schoolId), province);
                }
            }
        }

    }

    /**
     * 遍历HBase中表数据，根据学校计算用户所属省份，输出：省份、用户、角色信息
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
        if (province == null) return;
        context.write(new ArealRecord(province, role), new LongWritable(userId));
    }

}
