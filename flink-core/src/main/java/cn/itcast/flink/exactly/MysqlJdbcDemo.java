package cn.itcast.flink.exactly;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 采用JDBC方式向MySQL数据库写入数据，考虑事务性写入，手动编程
 * @author xuanyu
 */
public class MysqlJdbcDemo {

    /**
     * 向Mysql数据库写入数据，采用JDBC方式，5个步骤
     */
    public static void main(String[] args) throws SQLException {
        // 定义变量
        Connection connection = null;
        PreparedStatement pstmt = null ;

        try {
            // 1. 加载驱动类
            Class.forName("com.mysql.jdbc.Driver");
            // 2. 获取连接
            connection = DriverManager.getConnection("jdbc:...", "root", "123466");
            // todo step1. 开启事务
            connection.setAutoCommit(false);
            // 3. Statement对象
            pstmt = connection.prepareStatement(
                "INSERT INTO db_flink.t_student(id, name, age) VALUES (?, ?, ?)"
            );
            // 4. 执行
            pstmt.setInt(1, 31);
            pstmt.setString(2, "Jack");
            pstmt.setInt(3, 24);
            // todo step2. 预提交，将数据放入到事务中
            pstmt.execute();

            // todo step3. 提交，将数据提交给数据库服务
            connection.commit();
        }catch (Exception e){
            e.printStackTrace();
            // todo: step4. 事务回滚
            if(null != connection){
                connection.rollback();
            }
        }finally {
            // 5. 关闭连接
            if(null != pstmt){
                pstmt.close();
            }
            if(null != connection) {
                connection.close();
            }
        }
    }

}
