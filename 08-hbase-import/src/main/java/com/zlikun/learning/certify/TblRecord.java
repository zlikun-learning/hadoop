package com.zlikun.learning.certify;

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
 * @date 2018-02-23 10:25
 */
public class TblRecord implements Writable, DBWritable {

    public Long id;              // 认证ID
    public Long userId;         // 认证用户ID
    public Integer schoolId;    // 认证学校ID
    public Integer role;         // 认证身份：1学生、2教师
    public Long createTime;     // 认证时间，秒级时间戳

    public boolean isValid() {
        return this.id != null
                && this.userId != null
                && this.schoolId != null
                && this.role != null
                && this.createTime != null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeLong(userId);
        out.writeInt(schoolId);
        out.writeInt(role);
        out.writeLong(createTime);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readLong();
        this.userId = in.readLong();
        this.schoolId = in.readInt();
        this.role = in.readInt();
        this.createTime = in.readLong();
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {

    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.id = resultSet.getLong(1);
        this.userId = resultSet.getLong(2);
        this.schoolId = resultSet.getInt(3);
        this.role = resultSet.getInt(4);
        Date createTime = resultSet.getDate(5);
        if (createTime != null) {
            this.createTime = createTime.getTime() / 1000;
        }
    }
}
