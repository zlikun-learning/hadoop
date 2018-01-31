package com.zlikun.learning.logins;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * MySQL 表记录，用于读写MySQL表数据使用
 * RowKey设计：12位时间戳(精确到秒，左边补零) + 12位用户ID(左边补零)，目前只有根据时间统计的需求，暂无按用户统计的需求，固暂定按此设计，后期可以引入二级索引实现(非HBase原生，参考360的实现方案)
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-01-16 11:53
 */
public class TblRecord implements Writable, DBWritable {

    // 登录用户ID
    public Long userId;
    // 登录账号类型
    public String accountType;
    // 开放用户类型
    public String oauthType;
    // 登录IP、多个以逗号分隔
    public String addr;
    // 登录时间
    public Long loginTime = 0L;
    // App类型，仅针对APP
    public String appType;
    // App平台，仅针对APP
    public String appPlatform;
    // APP版本名
    public String appVersion;
    // APP版本号，必须为自然数
    public Integer appVersionNumber = 0;
    // APP设备号
    public String deviceNumber;
    // IMEI信息
    public String imei;
    // 运营商信息(网络)
    public String operatorInfo;
    // 系统版本号
    public String osVersion;
    // 系统内核信息
    public String kernelInfo;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // 注意将数字类型值放String类型前面填充，否则会出现EOFException异常
        dataOutput.writeLong(userId); // 登录用户ID，非空
        dataOutput.writeLong(loginTime); // 登录时间，转换为时间戳，不能为空(空数据应被忽略)
        dataOutput.writeInt(appVersionNumber); // 登录设备版本号(整型)，空则以"0"填充
        writeString(dataOutput, accountType);   // 登录帐号，空时以"UNKNOWN"填充
        writeString(dataOutput, oauthType); // 由OPEN_TYPE字段决定，可以为空
        writeString(dataOutput, addr); // 登录IP，可以为空
        writeString(dataOutput, appType); // 由CLIENT_CODE字段决定，不能为空，空则以"WEB"填充
        writeString(dataOutput, appPlatform); // 登录设备所属平台，不能为空，空则以"PC"填充
        writeString(dataOutput, appVersion);  // 登录设备版本名，允许为空
        writeString(dataOutput, deviceNumber); // 登录设备设备号，允许为空
        writeString(dataOutput, imei); // 登录设备IMEI信息，允许为空
        writeString(dataOutput, operatorInfo); // 网络运营商信息，允许为空
        writeString(dataOutput, osVersion); // 设备系统版本信息，允许为空
        writeString(dataOutput, kernelInfo); // 设备系统内含信息，允许为空
    }

    private void writeString(DataOutput output , String value) throws IOException {
        this.writeString(output, value, "");
    }

    private void writeString(DataOutput output , String value, String placeholder) throws IOException {
        WritableUtils.writeString(output, value != null ? value.trim() : placeholder);
    }

    private String readString(DataInput input) throws IOException {
        return WritableUtils.readString(input);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.userId = dataInput.readLong();
        this.loginTime = dataInput.readLong();
        this.appVersionNumber = dataInput.readInt();
        this.accountType = readString(dataInput);
        this.oauthType = readString(dataInput);
        this.addr = readString(dataInput);
        this.appType = readString(dataInput);
        this.appPlatform = readString(dataInput);
        this.appVersion = readString(dataInput);
        this.deviceNumber = readString(dataInput);
        this.imei = readString(dataInput);
        this.operatorInfo = readString(dataInput);
        this.osVersion = readString(dataInput);
        this.kernelInfo = readString(dataInput);
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        // 目前暂无向MySQL写入的需求
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        int index = 0 ;
        this.userId = resultSet.getLong(++ index);
        this.accountType = resultSet.getString(++ index);
        if (this.accountType == null) this.accountType = "UNKNOWN";
        Integer openType = resultSet.getInt(++ index);
        if (openType != null && openType > 0) {
            switch (openType) {
                case 1 : this.oauthType = "QQ"; break;
                case 2 : this.oauthType = "SECC"; break;
                case 3 : this.oauthType = "LNSC"; break;
                case 4 : this.oauthType = "CFFEX"; break;
                case 5 : this.oauthType = "WECHAT"; break;
                case 6 : this.oauthType = "WECHAT_GZH"; break;
                case 7 : this.oauthType = "GXU"; break;
                case 8 : this.oauthType = "WEIBO"; break;
            }
        }
        String clientCode = resultSet.getString(++ index);
        // 按新定义格式转换App信息
        if (clientCode != null) {
            switch (clientCode) {
                case "PC_WEB" : this.appType = "WEB"; this.appPlatform = "PC"; break;
                case "IOS_STUDENT_QUICK" : this.appType = "ZD_QUICK"; this.appPlatform = "IOS"; break;
                case "IOS_STUDENT_STORE" : this.appType = "ZD_STORE"; this.appPlatform = "IOS"; break;
                case "IOS_TEACHER_QUICK" : this.appType = "JSQ_QUICK"; this.appPlatform = "IOS"; break;
                case "IOS_TEACHER_STORE" : this.appType = "JSQ_STORE"; this.appPlatform = "IOS"; break;
                case "IOS_VIP_QUICK" : this.appType = "SX_QUICK"; this.appPlatform = "IOS"; break;
                case "ANDROID_STUDENT_STORE" : this.appType = "ZD_STORE"; this.appPlatform = "ANDROID"; break;
                case "ANDROID_TEACHER_STORE" : this.appType = "JSQ_STORE"; this.appPlatform = "ANDROID"; break;
                case "ANDROID_VIP_STORE" : this.appType = "SX_STORE"; this.appPlatform = "ANDROID"; break;
                case "ZHSW_ANDROID_STORE" : this.appType = "ZHSW_STORE"; this.appPlatform = "ANDROID"; break;
                case "ZHSW_IOS_STORE" : this.appType = "ZHSW_STORE"; this.appPlatform = "IOS"; break;
                case "ZD_ANDROID_STORE" : this.appType = "ZD_STORE"; this.appPlatform = "ANDROID"; break;
                case "ZD_IOS_STORE" : this.appType = "ZD_STORE"; this.appPlatform = "IOS"; break;
                case "ZD_IOS_QUICK" : this.appType = "ZD_QUICK"; this.appPlatform = "IOS"; break;
                case "JSQ_ANDROID_STORE" : this.appType = "JSQ_STORE"; this.appPlatform = "ANDROID"; break;
                case "JSQ_IOS_STORE" : this.appType = "JSQ_STORE"; this.appPlatform = "IOS"; break;
                case "JSQ_IOS_QUICK" : this.appType = "JSQ_QUICK"; this.appPlatform = "IOS"; break;
                case "SX_ANDROID_STORE" : this.appType = "SX_STORE"; this.appPlatform = "ANDROID"; break;
                case "SX_IOS_STORE" : this.appType = "SX_STORE"; this.appPlatform = "IOS"; break;
                case "SX_IOS_QUICK" : this.appType = "SX_QUICK"; this.appPlatform = "IOS"; break;
                case "ERP_ANDROID_QUICK" : this.appType = "XHZ_QUICK"; this.appPlatform = "ANDROID"; break;
                case "ERP_IOS_QUICK" : this.appType = "XHZ_QUICK"; this.appPlatform = "IOS"; break;
                default: this.appType = "WEB"; this.appPlatform = "PC"; break;
            }
        }
        this.appVersion = resultSet.getString(++ index);
        this.addr = resultSet.getString(++ index);
        Timestamp loginTime = resultSet.getTimestamp(++ index);
        if (loginTime != null) {
            this.loginTime = loginTime.getTime();
        }
    }

    /**
     * 该方法仅供调试使用
     * @return
     */
    @Override
    public String toString() {
        return this.userId + "," + this.appType + "," + this.appPlatform + "," + this.loginTime ;
    }

}
