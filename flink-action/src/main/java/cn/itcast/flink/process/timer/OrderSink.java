package cn.itcast.flink.process.timer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 将数据流保存至MySQL数据库表中
 * @author xuyuan
 */
public class OrderSink extends RichSinkFunction<OrderData> {
	// 定义变量
	private Connection conn = null ;
	private PreparedStatement pstmt = null ;

	@Override
	public void open(Configuration parameters) throws Exception {
		// a. 加载驱动类
		Class.forName("com.mysql.jdbc.Driver");
		// b. 获取连接
		conn = DriverManager.getConnection(
			"jdbc:mysql://node1.itcast.cn:3306/?useUnicode=true&characterEncoding=utf-8&useSSL=false",
			"root",
			"123456"
		);
		// c. 获取PreparedStatement实例
		pstmt = conn.prepareStatement("INSERT INTO db_flink.tbl_orders (order_id, user_id, order_time, order_status, order_amount) VALUES (?,?,?,?,?)") ;
	}

	@Override
	public void invoke(OrderData order, Context context) throws Exception {
		// d. 设置占位符值
		pstmt.setString(1, order.getOrderId());
		pstmt.setString(2, order.getUserId());
		pstmt.setString(3, order.getOrderTime());
		pstmt.setString(4, order.getOrderStatus());
		pstmt.setDouble(5, order.getOrderAmount());
		// e. 执行插入
		pstmt.executeUpdate();
	}

	@Override
	public void close() throws Exception {
		if(null != pstmt && ! pstmt.isClosed()) {
			pstmt.close();
		}
		if(null != conn && ! conn.isClosed()) {
			conn.close();
		}
	}

}