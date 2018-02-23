package com.zlikun.learning.user.areal;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
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
 * @date 2018-02-23 11:10
 */
public class ArealRecord implements Writable, DBWritable {

    public String province;
    public Integer role;
    public Integer users = 0;

    public ArealRecord() {
    }

    public ArealRecord(String province, Integer role) {
        this.province = province;
        this.role = role;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (!obj.getClass().equals(ArealRecord.class)) return false;
        ArealRecord target = (ArealRecord) obj;
        return this.province.equals(target.province) && this.role.equals(target.role);
    }

    @Override
    public int hashCode() {
        return this.province.hashCode() * 31 + this.role.hashCode();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, province);
        out.writeInt(role);
        out.writeInt(users);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.province = WritableUtils.readString(in);
        this.role = in.readInt();
        this.users = in.readInt();
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1, this.province);
        statement.setInt(2, this.role);
        statement.setInt(3, this.users);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {

    }
}
