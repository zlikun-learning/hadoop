package com.zlikun.learning.top;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 由于MR默认按键排序，这里使用值排序，所以构建一个新的键类型，将键值封装为一个新键，用于排序
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017-12-19 13:22
 */
public class NewKey implements WritableComparable<NewKey> {

    private String key;
    private int value;

    public NewKey() {
    }

    public NewKey(String key, int value) {
        this.key = key;
        this.value = value;
    }

    /**
     * 按键排序时会使用方法，这里只比较值，表示按值排序
     * @param newKey
     * @return
     */
    @Override
    public int compareTo(NewKey newKey) {
        // 注意两者顺序，这里按降序排序
        int tmp = Integer.compare(newKey.value, this.value) ;
        if (tmp != 0) return tmp;
        return this.getKey().compareTo(newKey.getKey());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // 经测试，字符串与数字同时写入时，数字总会变成4个字节的空白字符串(原因未知)，导致readFields时出错
        dataOutput.writeBytes(this.key);
        dataOutput.write('\r');
        dataOutput.writeInt(this.value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.key = dataInput.readLine();
        this.value = dataInput.readInt();
    }

    /**
     * 务必重写
     * @param obj
     * @return
     */
    @Override
    public boolean equals(Object obj) {
        NewKey newKey = (NewKey) obj;
        return newKey.getKey().equals(this.getKey());
    }

    /**
     * 务必重写
     * @return
     */
    @Override
    public int hashCode() {
        return this.getKey().hashCode();
    }

    public String getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }

    @Override
    public String toString() {
        return this.key + " = " + this.value;
    }
}
