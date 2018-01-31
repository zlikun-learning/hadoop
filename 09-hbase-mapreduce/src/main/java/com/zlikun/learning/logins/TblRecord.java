package com.zlikun.learning.logins;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-01-31 19:57
 */
public class TblRecord implements Writable, DBWritable {

    // 用户ID
    public Long userId;
    // 登录次数
    public Long loginTimes;

    public TblRecord() {
    }

    public TblRecord(Long userId, Long loginTimes) {
        this.userId = userId;
        this.loginTimes = loginTimes;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(userId);
        dataOutput.writeLong(loginTimes);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.userId = dataInput.readLong();
        this.loginTimes = dataInput.readLong();
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setLong(1, this.userId);
        statement.setLong(2, this.loginTimes);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        // 本案例仅使用写入，不使用读取
    }

    /**
     * 该方法仅供调试使用
     * @return
     */
    @Override
    public String toString() {
        return this.userId + ":" + this.loginTimes ;
    }

}
