package cn.itcast.flink.process.timer;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 使用Flink Stream中Timer定时器，实现电商系统未付款订单，超时自动取消
 * @author xuyuan
 */
public class StreamProcessTimerDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		// 使用嵌入式RocksDBState存储State数据，避免数据量太大，导致OOM
		env.setStateBackend(new EmbeddedRocksDBStateBackend(true)) ;

		// 2. 数据源-source
		DataStreamSource<String> orderStream = env.addSource(new OrderSource());
		// orderStream.printToErr();

		// 3. 数据转换-transformation
		/*
			订单数据 -> 2022080615071482818051,161059,2022-08-06 15:07:14.828,未付款,11.2
		 */
		SingleOutputStreamOperator<OrderData> processStream = orderStream
			.keyBy(line -> line.split(",")[0])
			.process(
				new KeyedProcessFunction<String, String, OrderData>() {
					private FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
					@Override
					public void processElement(String value, Context ctx, Collector<OrderData> out) throws Exception {
						// 解析数据，封装到实体类
						String[] array = value.split(",");
						// 封装实体类对象
						OrderData orderData = new OrderData();
						orderData.setOrderId(array[0]);
						orderData.setUserId(array[1]);
						orderData.setOrderTime(array[2]);
						orderData.setOrderStatus(array[3]);
						orderData.setOrderAmount(Double.parseDouble(array[4]));
						// 输出数据
						out.collect(orderData);

						// todo 判断订单状态，如果是未付款，创建定时任务，假设15s后触发执行 -> 再次依据订单id查询订单状态，如果依然时未付款，直接取消订单（修改订单状态为取消）
						if("未付款".equals(orderData.getOrderStatus())){
							System.out.println("订单[" + orderData.getOrderId() + "]状态为【未付款】, 设置定时任务，15秒后触发执行......");
							// 创建定时任务，到达时间，触发执行onTimer方法
							long time = fastDateFormat.parse(orderData.getOrderTime()).getTime() ;
							ctx.timerService().registerProcessingTimeTimer(time + 15 * 1000L);
						}
					}

					// 当定时器触发执行时，调用OnTimer方法，实现订单超时自动取消功能
					@Override
					public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderData> out) throws Exception {
						// a. 获取订单id
						String orderId = ctx.getCurrentKey();
						System.out.println("指定定时任务，检查订单[" + orderId + "]状态....................");
						// b. 依据订单id查询Mysql数据库订单状态
						String orderStatus = queryStatus(orderId);
						System.out.println("查询订单[" + orderId + "]状态为：" + orderStatus + "....................");
						// c. 判断状态：未付款，更新订单状态为：取消
						if("未付款".equals(orderStatus)){
							updateStatus(orderId);
							System.out.println("订单[" + orderId + "]已超时， 更新状态为：取消....................................");
						}
					}

					// 依据orderId查询订单状态
					private String queryStatus(String orderId) throws Exception{
						// a. 加载驱动类，获取连接
						Class.forName("com.mysql.jdbc.Driver");
						Connection conn = DriverManager.getConnection(
							"jdbc:mysql://node1.itcast.cn:3306/?useUnicode=true&characterEncoding=utf-8&useSSL=false",
							"root", "123456"
						);
						// b. 执行查询
						PreparedStatement pstmt = conn.prepareStatement("SELECT order_status FROM db_flink.tbl_orders WHERE order_id = ?") ;
						pstmt.setString(1, orderId);
						ResultSet result = pstmt.executeQuery();
						// c. 获取订单状态
						String orderStatus = "unknown";
						while (result.next()){
							orderStatus = result.getString(1);
						}
						// d. 关闭连接
						result.close();
						pstmt.close();
						conn.close();
						// e. 返回
						return orderStatus ;
					}

					// 依据orderId更新订单状态为：取消
					private void updateStatus(String orderId) throws Exception{
						// a. 加载驱动类，获取连接
						Class.forName("com.mysql.jdbc.Driver");
						Connection conn = DriverManager.getConnection(
							"jdbc:mysql://node1.itcast.cn:3306/?useUnicode=true&characterEncoding=utf-8&useSSL=false",
							"root", "123456"
						);
						// b. 执行更新
						PreparedStatement pstmt = conn.prepareStatement("UPDATE db_flink.tbl_orders SET order_status = ? WHERE order_id = ?") ;
						pstmt.setString(1, "取消");
						pstmt.setString(2, orderId);
						pstmt.executeUpdate();
						// c. 关闭连接
						pstmt.close();
						conn.close();
					}
				}
			);

		// 4. 数据终端-sink
		processStream.addSink(new OrderSink()) ;

		// 5. 触发执行-execute
		env.execute("StreamProcessTimerDemo");
	}

}  