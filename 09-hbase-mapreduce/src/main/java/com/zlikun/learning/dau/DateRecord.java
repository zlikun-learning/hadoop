package com.zlikun.learning.dau;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 日期与App类型组合值
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-09 10:11
 */
public class DateRecord implements Writable, DBWritable, WritableComparable<DateRecord> {

    public String date;
    public String type;
    public int users;   // 人数
    public int count;   // 人次

    public DateRecord() {
    }

    public DateRecord(String date, String type) {
        this.date = date;
        this.type = type;
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1, this.date);
        statement.setString(2, this.type);
        statement.setInt(3, users);
        statement.setInt(4, count);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        // TODO ...
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, date);
        WritableUtils.writeString(out, type);
        out.writeInt(users);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.date = WritableUtils.readString(in);
        this.type = WritableUtils.readString(in);
        this.users = in.readInt();
        this.count = in.readInt();
    }

    @Override
    public int compareTo(DateRecord o) {
        int val = this.date.compareTo(o.date) ;
        if (val != 0) return val;
        return this.type.compareTo(o.type);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof DateRecord) || !(obj.getClass().equals(this.getClass()))) {
            return false;
        }
        DateRecord dr = (DateRecord) obj;
        return this.date.equals(dr.date) && this.type.equals(dr.type);
    }

    @Override
    public int hashCode() {
        return this.date.hashCode() * 31 + this.type.hashCode();
    }

    @Override
    public String toString() {
        return this.date + ":" + this.type + "=>(" + this.users + "/" + this.count + ")";
    }

}
