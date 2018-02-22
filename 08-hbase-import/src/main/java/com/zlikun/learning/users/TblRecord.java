package com.zlikun.learning.users;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-22 16:59
 */
public class TblRecord implements Writable, DBWritable {

    public Long userId;         // 注册用户ID
    public Integer joinDate;    // 注册日期(格式：yyyyMMdd，示例：20180121)

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(userId);
        out.writeInt(joinDate);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.userId = in.readLong();
        this.joinDate = in.readInt();
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {

    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.userId = resultSet.getLong(1);
        Date createTime = resultSet.getDate(2);
        if (createTime != null) {
            this.joinDate = NumberUtils.toInt(DateFormatUtils.format(createTime, "yyyyMMdd"));
        }
    }
}
