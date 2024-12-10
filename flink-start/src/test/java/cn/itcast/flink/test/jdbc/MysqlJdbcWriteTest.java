package cn.itcast.flink.test.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 基于JDBC方式写入数据到Mysql数据库表中
 *
 * @author xuanyu
 */
public class MysqlJdbcWriteTest {

    public static void main(String[] args) throws Exception {
        // step1. 加载驱动
        Class.forName("com.mysql.jdbc.Driver");

        // step2. 获取连接
        Connection connection = DriverManager.getConnection(
            "jdbc:mysql://node1.itcast.cn:3306/?useSSL=false", "root", "123456"
        );

        // step3. 创建Statement对象
        PreparedStatement pstmt = connection.prepareStatement(
            "INSERT INTO db_flink.t_student(id, name, age) VALUES (?, ?, ?)"
        );

        // step4. 执行操作
        pstmt.setInt(1, 99);
        pstmt.setString(2, "Jetty");
        pstmt.setInt(3, 88);
        pstmt.execute();

        // step5. 关闭连接
        pstmt.close();
        connection.close();
    }

}
