package com.zlikun.learning.mysql.logins;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;

/**
 * MySQL 表记录，用于读写MySQL表数据使用
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-01-16 11:53
 */
public class TblRecord implements Writable, DBWritable {

    Long userId ;   // 用户ID
    Long days;      // 登录日期距1970/1/1天数(方便后续按序列处理数据，以统计连续登录天数)

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.userId);
        dataOutput.writeLong(this.days);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.userId = dataInput.readLong();
        this.days = dataInput.readLong();
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setLong(1, this.userId);
        statement.setLong(2, this.days);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.userId = resultSet.getLong(1);
        Timestamp loginTime = resultSet.getTimestamp(2);
        if (loginTime != null) {
            this.days = Instant.ofEpochMilli(loginTime.getTime()).atZone(ZoneId.systemDefault()).toLocalDate().toEpochDay();
        } else {
            this.days = -1L;
        }
    }

    @Override
    public String toString() {
        return this.userId + ":" + this.days;
    }
}
