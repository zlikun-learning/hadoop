package com.zlikun.learning.user.areal;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-23 11:10
 */
public class ArealRecord implements Writable, DBWritable, WritableComparable<ArealRecord> {

    public String province;
    public Integer users = 0;
    public Long date;

    public ArealRecord() {
    }

    public ArealRecord(String province) {
        this.province = province;
    }

    @Override
    public String toString() {
        return "[" + new Date(this.date) + "] " + this.province + "=>" + this.users;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (!obj.getClass().equals(ArealRecord.class)) return false;
        if (!(obj instanceof ArealRecord)) return false;
        ArealRecord target = (ArealRecord) obj;
        return this.province.equals(target.province) ;
    }

    @Override
    public int hashCode() {
        return this.province.hashCode() ;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, province);
        out.writeInt(users);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.province = WritableUtils.readString(in);
        this.users = in.readInt();
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1, this.province);
        statement.setInt(2, this.users);
        statement.setDate(3, new Date(date));
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {

    }

    /**
     * 必须实现 WritableComparable 接口，否则可能出现：ClassCastException 异常
     * @param o
     * @return
     */
    @Override
    public int compareTo(ArealRecord o) {
        if (o == null) return -1;
        return this.province.compareTo(o.province);
    }
}
