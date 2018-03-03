package com.zlikun.learning;

import org.apache.phoenix.jdbc.PhoenixDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/3/3 15:47
 */
public class JdbcTest {

    private Connection connection;

    @Before
    public void init() throws ClassNotFoundException, SQLException {
        Class.forName(PhoenixDriver.class.getName());
        // get connection
        // jdbc 的 url 类似为 jdbc:phoenix [ :<zookeeper quorum> [ :<port number> ] [ :<root node> ] ]，
        // 需要引用三个参数：hbase.zookeeper.quorum、hbase.zookeeper.property.clientPort、and zookeeper.znode.parent，
        // 这些参数可以缺省不填而在 hbase-site.xml 中定义
        connection = DriverManager.getConnection("jdbc:phoenix:hbase.zlikun.com:2181");
    }

    @After
    public void destroy() throws SQLException {
        connection.close();
    }

    @Test
    public void create() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE IF NOT EXISTS BOOK (ID INTEGER PRIMARY KEY ,NAME VARCHAR ,PRICE INTEGER)");
        connection.commit();
        statement.close();
    }

    /**
     * 实现插入和更新功能
     * @throws SQLException
     */
    @Test
    public void upsert() throws SQLException {
        PreparedStatement statement = connection.prepareStatement("UPSERT INTO BOOK (ID, NAME, PRICE) VALUES (?, ?, ?)");
        statement.setInt(1, 1);
        statement.setString(2, "HBase权威指南");
        statement.setInt(3, 7800);
        statement.executeUpdate();
        connection.commit();    // 需要提交事务，否则数据可能不会被插入
        statement.close();
    }

    @Test
    public void select() throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM BOOK");
        while (resultSet.next()) {
            System.out.printf("ID = %d, NAME = %s, PRICE = %d\n",
                    resultSet.getInt(1), resultSet.getString(2), resultSet.getInt(3));
        }
        statement.close();
    }

    @Test
    public void delete() throws SQLException {
        Statement statement = connection.createStatement();
        statement.executeUpdate("DELETE FROM BOOK WHERE ID = 1");
        connection.rollback();      // 事务回滚
        statement.close();
    }

    /**
     * 删除表(不支持事务，实际会删除HBase中的表)
     * @throws SQLException
     */
    @Test @Ignore
    public void drop() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("DROP TABLE BOOK");
        statement.close();
    }

}
