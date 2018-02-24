package com.zlikun.learning.user.role;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-24 13:21
 */
public class RoleRecord implements Writable, DBWritable, WritableComparable<RoleRecord> {

    // 角色：1学生、2教师
    public Integer role;
    // 用户人数
    public Integer users = 0;
    public Long date;

    public RoleRecord() {
    }

    public RoleRecord(Integer role) {
        this.role = role;
    }

    @Override
    public int compareTo(RoleRecord o) {
        return role.compareTo(o.role);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(role);
        out.writeInt(users);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        role = in.readInt();
        users = in.readInt();
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setInt(1, role);
        statement.setInt(2, users);
        statement.setDate(3, new Date(date));
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {

    }
}
