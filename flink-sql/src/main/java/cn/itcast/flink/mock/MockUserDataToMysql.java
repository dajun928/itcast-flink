package cn.itcast.flink.mock;

import cn.binarywang.tools.generator.ChineseAddressGenerator;
import cn.binarywang.tools.generator.ChineseIDCardNumberGenerator;
import cn.binarywang.tools.generator.ChineseMobileNumberGenerator;
import cn.binarywang.tools.generator.ChineseNameGenerator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Random;

/**
 * 模拟产生用户信息数据，插入到MySQL表中
 * @author xuanyu
 */
public class MockUserDataToMysql {

    public static void main(String[] args) throws Exception{
        // 1. 加载驱动类
        Class.forName("com.mysql.jdbc.Driver");
        // 2. 获取连接
        Connection connection = DriverManager.getConnection(
            "jdbc:mysql://node1.itcast.cn:3306/?characterEncoding=utf8&useSSL=false", "root", "123456"
        );
        // 3. 获取Statement对象
        PreparedStatement pstmt = connection.prepareStatement(
            "INSERT INTO db_flink.tbl_users_dim(user_id, user_name, id_card, mobile, address, gender) \n" +
            "VALUES (?, ?, ?, ?, ?, ?)"
        );

        Random random = new Random();
        int index = 0;
        String[] genderArray = new String[]{"男", "女", "男", "女", "女", "女"};
        while(index < 1000){
            // 用户ID
            String userId = "user_" + (1000 + (index++));
            // 中国常见姓名
            String name = ChineseNameGenerator.getInstance().generate();
            // 模拟中国身份证号码
            String idCard = ChineseIDCardNumberGenerator.getInstance().generate();
            // 模拟中国手机号
            String mobile = ChineseMobileNumberGenerator.getInstance().generate();
            // 模拟中国地址
            String address = ChineseAddressGenerator.getInstance().generate();
            // 模拟性别
            String gender = genderArray[random.nextInt(genderArray.length)];

            // 4. 设置值
            pstmt.setString(1, userId);
            pstmt.setString(2, name);
            pstmt.setString(3, idCard);
            pstmt.setString(4, mobile);
            pstmt.setString(5, address);
            pstmt.setString(6, gender);
            pstmt.addBatch();
        }
        // 批次插入
        pstmt.executeBatch() ;

        // 5. 关闭连接
        pstmt.close();
        connection.close();
    }

}
