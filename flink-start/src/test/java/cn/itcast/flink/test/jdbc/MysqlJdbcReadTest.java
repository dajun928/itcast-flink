package cn.itcast.flink.test.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 基于JDBC方式读取Mysql数据库表中数据
 *
 * @author xuanyu
 */
public class MysqlJdbcReadTest {

    public static void main(String[] args) throws Exception {
        // step1. 加载驱动
        Class.forName("com.mysql.jdbc.Driver");
        // step2. 获取连接
        Connection connection = DriverManager.getConnection(
            "jdbc:mysql://node1.itcast.cn:3306/?useSSL=false", "root", "123456"
        );
        // step3. 创建Statement对象
        PreparedStatement pstmt = connection.prepareStatement("SELECT id, name, age FROM db_flink.t_student");
        // step4. 执行操作
        ResultSet result = pstmt.executeQuery();
        // step5. 获取数据
        while (result.next()) {
            // 获取每条数据
            int id = result.getInt("id");
            String name = result.getString("name");
            int age = result.getInt("age");
            System.out.println("id = " + id + ", name = " + name + ", age = " + age);
        }
        // step6. 关闭连接
        result.close();
        pstmt.close();
        connection.close();
    }

}
